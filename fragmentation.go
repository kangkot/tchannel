package tchannel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"code.uber.internal/personal/mmihic/tchannel-go/typed"
)

var (
	// Peer sent a different checksum type for a continuation fragment
	ErrMismatchedChecksumTypes = errors.New("peer sent a different checksum type for fragment")

	// Caller attempted to write to a body after the last fragment was sent
	ErrWriteAfterComplete = errors.New("attempted to write to a stream after the last fragment sent")

	// Local checksum calculation differs from that reported by peer
	ErrMismatchedChecksum = errors.New("local checksum differs from peer")

	// Caller considers an argument complete, but there is more data remaining in the argument
	ErrDataLeftover = errors.New("more data remaining in argument")

	errTooLarge                   = errors.New("impl error, data exceeds remaining fragment size")
	errAlignedAtEndOfOpenFragment = errors.New("impl error; align-at-end of open fragment")
	errNoOpenChunk                = errors.New("impl error, writeChunkData or endChunk called with no open chunk")
	errChunkAlreadyOpen           = errors.New("impl error, beginChunk called with an already open chunk")
)

const (
	// Flag indicating there are more fragments to come
	flagMoreFragments = 0x01
)

// An outbound fragment is a fragment being sent to a peer
type outFragment struct {
	frame         *Frame
	checksum      Checksum
	checksumBytes []byte
	chunkStart    []byte
	chunkSize     int
	remaining     []byte
}

// Returns the number of bytes remaining in the fragment
func (f *outFragment) bytesRemaining() int {
	return len(f.remaining)
}

// Finishes a fragment, optionally marking it as the last fragment
func (f *outFragment) finish(last bool) *Frame {
	// If we still have a chunk open, close it before finishing the fragment
	if f.chunkOpen() {
		f.endChunk()
	}

	if last {
		f.frame.Payload[0] &= ^byte(flagMoreFragments)
	} else {
		f.frame.Payload[0] |= flagMoreFragments
	}

	copy(f.checksumBytes, f.checksum.Sum())
	f.frame.Header.Size = uint16(len(f.frame.Payload) - len(f.remaining))
	return f.frame
}

// Writes data for a chunked part into the fragment.  The data must fit into the fragment
func (f *outFragment) writeChunkData(b []byte) (int, error) {
	if len(b) > len(f.remaining) {
		return 0, errTooLarge
	}

	if len(f.chunkStart) == 0 {
		return 0, errNoOpenChunk
	}

	copy(f.remaining, b)
	f.remaining = f.remaining[len(b):]
	f.chunkSize += len(b)
	f.checksum.Add(b)
	return len(b), nil
}

// Returns true if the fragment can fit a new chunk
func (f *outFragment) canFitNewChunk() bool {
	return len(f.remaining) > 2
}

// Begins a new chunk at the current location in the fragment
func (f *outFragment) beginChunk() error {
	if f.chunkOpen() {
		return errChunkAlreadyOpen
	}

	f.chunkStart = f.remaining[0:2]
	f.chunkSize = 0
	f.remaining = f.remaining[2:]
	return nil
}

// Ends a previously opened chunk, recording the chunk size
func (f *outFragment) endChunk() error {
	if !f.chunkOpen() {
		return errNoOpenChunk
	}

	binary.BigEndian.PutUint16(f.chunkStart, uint16(f.chunkSize))
	f.chunkStart = nil
	f.chunkSize = 0
	return nil
}

// Returns true if the fragment has a chunk open
func (f *outFragment) chunkOpen() bool { return len(f.chunkStart) > 0 }

// Creates a new outFragment around a frame and message, with a running checksum
func newOutboundFragment(frame *Frame, msg Message, checksum Checksum) (*outFragment, error) {
	f := &outFragment{
		frame:    frame,
		checksum: checksum,
	}
	f.frame.Header.Id = msg.Id()
	f.frame.Header.Type = msg.Type()

	wbuf := typed.NewWriteBuffer(f.frame.Payload[:])

	// Reserve fragment flag
	if err := wbuf.WriteByte(0); err != nil {
		return nil, err
	}

	// Write message specific header
	if err := msg.write(wbuf); err != nil {
		return nil, err
	}

	// Write checksum type and reserve bytes needed
	if err := wbuf.WriteByte(byte(f.checksum.TypeCode())); err != nil {
		return nil, err
	}

	f.remaining = f.frame.Payload[wbuf.CurrentPos():]
	f.checksumBytes = f.remaining[:f.checksum.TypeCode().ChecksumSize()]

	// Everything remaining is available for content
	f.remaining = f.remaining[f.checksum.TypeCode().ChecksumSize():]
	return f, nil
}

// A pseudo-channel for sending fragments to a remote peer.
// TODO(mmihic): Not happy with this name, or with this exact interface
type outFragmentChannel interface {
	// Opens a fragment for sending.  If there is an existing incomplete fragment on the channel,
	// that fragment will be returned.  Otherwise a new fragment is allocated
	beginFragment() (*outFragment, error)

	// Ends the currently open fragment, optionally marking it as the last fragment
	flushFragment(f *outFragment, last bool) error
}

// An multiPartWriter is an io.Writer for a collection of parts, capable of breaking
// large part into multiple chunks spread across several fragments.  Upstream code can
// send part data via the standard io.Writer interface, but should call endPart to
// indicate when they are finished with the current part.
type multiPartWriter struct {
	fragments   outFragmentChannel
	fragment    *outFragment
	alignsAtEnd bool
	complete    bool
}

// Creates a new multiPartWriter that creates and sends fragments through the provided channel.
func newMultiPartWriter(ch outFragmentChannel) *multiPartWriter {
	return &multiPartWriter{fragments: ch}
}

// Writes an entire part
func (w *multiPartWriter) WritePart(output Output, last bool) error {
	if err := output.WriteTo(w); err != nil {
		return err
	}

	return w.endPart(last)
}

// Writes part bytes, potentially splitting them across fragments
func (w *multiPartWriter) Write(b []byte) (int, error) {
	if w.complete {
		return 0, ErrWriteAfterComplete
	}

	written := 0
	for len(b) > 0 {
		// Make sure we have a fragment and an open chunk
		if err := w.ensureOpenChunk(); err != nil {
			return written, err
		}

		bytesRemaining := w.fragment.bytesRemaining()
		if bytesRemaining < len(b) {
			// Not enough space remaining in this fragment - write what we can, finish this fragment,
			// and start a new fragment for the remainder of the part
			if n, err := w.fragment.writeChunkData(b[:bytesRemaining]); err != nil {
				return written + n, err
			}

			if err := w.finishFragment(false); err != nil {
				return written, err
			}

			written += bytesRemaining
			b = b[bytesRemaining:]
		} else {
			// Enough space remaining in this fragment - write the full chunk and be done with it
			if n, err := w.fragment.writeChunkData(b); err != nil {
				return written + n, err
			}

			written += len(b)
			w.alignsAtEnd = w.fragment.bytesRemaining() == 0
			b = nil
		}
	}

	// If the fragment is complete, send it immediately
	if w.fragment.bytesRemaining() == 0 {
		if err := w.finishFragment(false); err != nil {
			return written, err
		}
	}

	return written, nil
}

// Ensures that we have a fragment and an open chunk
func (w *multiPartWriter) ensureOpenChunk() error {
	for {
		// No fragment - start a new one
		if w.fragment == nil {
			var err error
			if w.fragment, err = w.fragments.beginFragment(); err != nil {
				return err
			}
		}

		// Fragment has an open chunk - we are good to go
		if w.fragment.chunkOpen() {
			return nil
		}

		// Fragment can fit a new chunk - start it and hand off
		if w.fragment.canFitNewChunk() {
			w.fragment.beginChunk()
			return nil
		}

		// Fragment cannot fit the new chunk - finish the current fragment and get a new one
		if err := w.finishFragment(false); err != nil {
			return err
		}
	}
}

// Finishes with the current fragment, closing any open chunk and sending the fragment down the channel
func (w *multiPartWriter) finishFragment(last bool) error {
	w.fragment.endChunk()
	if err := w.fragments.flushFragment(w.fragment, last); err != nil {
		w.fragment = nil
		return err
	}

	w.fragment = nil
	return nil
}

// Marks the part as being complete.  If last is true, this is the last part in the message
func (w *multiPartWriter) endPart(last bool) error {
	if w.alignsAtEnd {
		// The last part chunk aligned with the end of a fragment boundary - send another fragment
		// containing an empty chunk so readers know the part is complete
		if w.fragment != nil {
			return errAlignedAtEndOfOpenFragment
		}

		var err error
		w.fragment, err = w.fragments.beginFragment()
		if err != nil {
			return err
		}

		w.fragment.beginChunk()
	}

	if w.fragment.chunkOpen() {
		w.fragment.endChunk()
	}

	if last {
		if err := w.fragments.flushFragment(w.fragment, true); err != nil {
			return err
		}

		w.complete = true
	}

	return nil
}

// An inFragment is a fragment received from a peer
type inFragment struct {
	frame    *Frame   // The frame containing the fragment
	last     bool     // true if this is the last fragment from the peer for this message
	checksum Checksum // Checksum for the fragment chunks
	chunks   [][]byte // The part chunks contained in the fragment
}

// Creates a new inFragment from an incoming frame and an expected message
func newInboundFragment(frame *Frame, msg Message, checksum Checksum) (*inFragment, error) {
	f := &inFragment{
		frame:    frame,
		checksum: checksum,
	}

	payload := f.frame.Payload[:f.frame.Header.Size]
	rbuf := typed.NewReadBuffer(payload)

	// Fragment flags
	flags, err := rbuf.ReadByte()
	if err != nil {
		return nil, err
	}

	f.last = (flags & flagMoreFragments) == 0

	// Message header
	if err := msg.read(rbuf); err != nil {
		return nil, err
	}

	// Read checksum type and bytes
	checksumType, err := rbuf.ReadByte()
	if err != nil {
		return nil, err
	}

	if f.checksum == nil {
		f.checksum = ChecksumType(checksumType).New()
	} else if ChecksumType(checksumType) != checksum.TypeCode() {
		return nil, ErrMismatchedChecksumTypes
	}

	peerChecksum, err := rbuf.ReadBytes(f.checksum.TypeCode().ChecksumSize())
	if err != nil {
		return nil, err
	}

	// Slice the remainder into chunks and confirm checksum
	for rbuf.BytesRemaining() > 0 {
		chunkSize, err := rbuf.ReadUint16()
		if err != nil {
			return nil, err
		}

		chunkBytes, err := rbuf.ReadBytes(int(chunkSize))
		if err != nil {
			return nil, err
		}

		f.chunks = append(f.chunks, chunkBytes)
		f.checksum.Add(chunkBytes)
	}

	// Compare checksums
	if bytes.Compare(peerChecksum, f.checksum.Sum()) != 0 {
		return nil, ErrMismatchedChecksum
	}

	return f, nil
}

// Consumes the next chunk in the fragment
func (f *inFragment) nextChunk() []byte {
	if len(f.chunks) == 0 {
		return nil
	}

	chunk := f.chunks[0]
	f.chunks = f.chunks[1:]
	return chunk
}

// returns true if there are more chunks remaining in the fragment
func (f *inFragment) hasMoreChunks() bool {
	return len(f.chunks) > 0
}

// Psuedo-channel for receiving inbound fragments from a peer
type inFragmentChannel interface {
	// Waits for a fragment to become available.  May return immediately if there is already an open unconsumed
	// fragment, or block until the next fragment appears
	waitForFragment() (*inFragment, error)
}

// An multiPartReader is an io.Reader for an individual TChannel part, capable of reading large
// part that have been split across fragments.  Upstream code can use the multiPartReader like
// a regular io.Reader to extract the bytes part, and should call endPart when they have finished
// reading a given part, to prepare the stream for the next part.
type multiPartReader struct {
	fragments           inFragmentChannel
	chunk               []byte
	lastChunkInFragment bool
	lastPartInMessage   bool
}

// Reads an input part from the stream
func (r *multiPartReader) ReadPart(input Input, last bool) error {
	if err := input.ReadFrom(r); err != nil {
		return err
	}

	return r.endPart()
}

func (r *multiPartReader) Read(b []byte) (int, error) {
	totalRead := 0

	for len(b) > 0 {
		if len(r.chunk) == 0 {
			if r.lastChunkInFragment {
				// We've already consumed the last chunk for this part
				return totalRead, io.EOF
			}

			nextFragment, err := r.fragments.waitForFragment()
			if err != nil {
				return totalRead, err
			}

			r.chunk = nextFragment.nextChunk()
			r.lastChunkInFragment = nextFragment.hasMoreChunks() // Remaining chunks are for other args
		}

		read := copy(b, r.chunk)
		totalRead += read
		r.chunk = r.chunk[read:]
		b = b[read:]
	}

	return totalRead, nil
}

// Marks the current part as complete, confirming that we've read the entire part and have nothing left over
func (r *multiPartReader) endPart() error {
	if len(r.chunk) > 0 {
		return ErrDataLeftover
	}

	if !r.lastChunkInFragment && !r.lastPartInMessage {
		// We finished on a fragment boundary - get the next fragment and confirm there is only a zero
		// length chunk header
		nextFragment, err := r.fragments.waitForFragment()
		if err != nil {
			return err
		}

		r.chunk = nextFragment.nextChunk()
		if len(r.chunk) > 0 {
			return ErrDataLeftover
		}
	}

	if r.lastPartInMessage {
		// TODO(mmihic): Confirm no more chunks in fragment
		// TODO(mmihic): Confirm no more fragments in message
	}

	return nil
}

func newMultiPartReader(ch inFragmentChannel, last bool) *multiPartReader {
	return &multiPartReader{fragments: ch, lastPartInMessage: last}
}
