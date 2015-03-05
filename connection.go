package tchannel

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	_ "time"

	"code.google.com/p/go.net/context"
	"github.com/op/go-logging"

	"code.uber.internal/infra/mmihic/tchannel-go/typed"
)

// Information about a TChannel peer
type PeerInfo struct {
	// The host and port that can be used to contact the peer, as encoded by net.JoinHostPort
	HostPort string

	// The logical process name for the peer, used for only for logging / debugging
	ProcessName string
}

func (p PeerInfo) String() string {
	return fmt.Sprintf("%s(%s)", p.HostPort, p.ProcessName)
}

const CurrentProtocolVersion = 0x02

var (
	ErrConnectionClosed            = errors.New("connection is closed")
	ErrConnectionNotReady          = errors.New("connection is not yet ready")
	ErrConnectionAlreadyActive     = errors.New("connection is already active")
	ErrConnectionWaitingOnPeerInit = errors.New("connection is waiting for the peer to sent init")
	ErrSendBufferFull              = errors.New("connection send buffer is full, cannot send frame")
	ErrRecvBufferFull              = errors.New("connection recv buffer is full, cannot recv frame")
)

// Options used during the creation of a TChannelConnection
type TChannelConnectionOptions struct {
	// The identity of the local peer
	PeerInfo PeerInfo

	// The frame pool, allowing better management of frame buffers.  Defaults to using raw heap
	FramePool FramePool

	// The size of receive channel buffers.  Defaults to 512
	RecvBufferSize int

	// The size of send channel buffers.  Defaults to 512
	SendBufferSize int

	// The type of checksum to use when sending messages
	ChecksumType ChecksumType
}

// A connection to a remote peer.
type TChannelConnection struct {
	ch             *TChannel
	log            *logging.Logger
	checksumType   ChecksumType
	framePool      FramePool
	conn           net.Conn
	localPeerInfo  PeerInfo
	remotePeerInfo PeerInfo
	sendCh         chan *Frame // channel for sending frames
	state          connectionState
	stateMut       sync.RWMutex
	reqMut         sync.Mutex
	activeResChs   map[uint32]chan<- *Frame // map of frame channels for incoming requests
	inbound        *inboundCallPipeline
	nextMessageId  uint32
}

type connectionState int

const (
	// TChannelConnection initiated by peer is waiting to recv init-req from peer
	connectionWaitingToRecvInitReq connectionState = iota

	// TChannelConnection initated by current process is waiting to send init-req to peer
	connectionWaitingToSendInitReq

	// TChannelConnection initiated by current process has sent init-req, and is waiting for init-req
	connectionWaitingToRecvInitRes

	// TChannelConnection is fully active
	connectionActive

	// TChannelConnection is starting to close; new incoming requests are rejected, outbound
	// requests are allowed to proceed
	connectionStartClose

	// TChannelConnection has finished processing all active inbound, and is waiting for outbound
	// requests to complete or timeout
	connectionInboundClosed

	// TChannelConnection is fully closed
	connectionClosed
)

// Creates a new TChannelConnection around an outbound connection initiated to a peer
func newOutboundConnection(ch *TChannel, conn net.Conn,
	opts *TChannelConnectionOptions) (*TChannelConnection, error) {
	c := newConnection(ch, conn, connectionWaitingToSendInitReq, opts)
	return c, nil
}

// Creates a new TChannelConnection based on an incoming connection from a peer
func newInboundConnection(ch *TChannel, conn net.Conn,
	opts *TChannelConnectionOptions) (*TChannelConnection, error) {
	c := newConnection(ch, conn, connectionWaitingToRecvInitReq, opts)
	return c, nil
}

// Creates a new connection in a given initial state
func newConnection(ch *TChannel, conn net.Conn, initialState connectionState,
	opts *TChannelConnectionOptions) *TChannelConnection {

	if opts == nil {
		opts = &TChannelConnectionOptions{}
	}

	sendBufferSize := opts.SendBufferSize
	if sendBufferSize <= 0 {
		sendBufferSize = 512
	}

	recvBufferSize := opts.RecvBufferSize
	if recvBufferSize <= 0 {
		recvBufferSize = 512
	}

	framePool := opts.FramePool
	if framePool == nil {
		framePool = DefaultFramePool
	}

	c := &TChannelConnection{
		ch:            ch,
		log:           ch.log,
		conn:          conn,
		framePool:     framePool,
		state:         initialState,
		sendCh:        make(chan *Frame, sendBufferSize),
		activeResChs:  make(map[uint32]chan<- *Frame),
		localPeerInfo: opts.PeerInfo,
		checksumType:  opts.ChecksumType,
	}

	// TODO(mmihic): Possibly defer until after handshake is successful
	c.inbound = newInboundCallPipeline(c.sendCh, &ch.handlers, framePool, ch.log)

	go c.readFrames()
	go c.writeFrames()
	return c
}

// Initiates a handshake with a peer.
func (c *TChannelConnection) sendInit(ctx context.Context) error {
	err := c.withStateLock(func() error {
		switch c.state {
		case connectionWaitingToSendInitReq:
			c.state = connectionWaitingToRecvInitRes
			return nil
		case connectionWaitingToRecvInitReq:
			return ErrConnectionWaitingOnPeerInit
		case connectionClosed, connectionStartClose, connectionInboundClosed:
			return ErrConnectionClosed
		case connectionActive, connectionWaitingToRecvInitRes:
			return ErrConnectionAlreadyActive
		default:
			return fmt.Errorf("connection in unknown state %d", c.state)
		}
	})
	if err != nil {
		return err
	}

	initMsgId := c.NextMessageId()
	initResCh := make(chan *Frame)
	c.withReqLock(func() error {
		c.activeResChs[initMsgId] = initResCh
		return nil
	})

	req := InitReq{initMessage{id: initMsgId}}
	req.Version = CurrentProtocolVersion
	req.InitParams = InitParams{
		InitParamHostPort:    c.localPeerInfo.HostPort,
		InitParamProcessName: c.localPeerInfo.ProcessName,
	}

	if err := c.sendMessage(&req); err != nil {
		c.outboundCallComplete(initMsgId)
		return c.connectionError(err)
	}

	res := InitRes{initMessage{id: initMsgId}}
	err = c.recvMessage(ctx, &res, initResCh)
	c.outboundCallComplete(initMsgId)
	if err != nil {
		return c.connectionError(err)
	}

	if res.Version != CurrentProtocolVersion {
		return c.connectionError(fmt.Errorf("Unsupported protocol version %d from peer", res.Version))
	}

	c.remotePeerInfo.HostPort = res.InitParams[InitParamHostPort]
	c.remotePeerInfo.ProcessName = res.InitParams[InitParamProcessName]

	c.withStateLock(func() error {
		if c.state == connectionWaitingToRecvInitRes {
			c.state = connectionActive
		}
		return nil
	})

	return nil
}

// Handles an incoming InitReq.  If we are waiting for the peer to send us an InitReq, and the
// InitReq is valid, send a corresponding InitRes and mark ourselves as active
func (c *TChannelConnection) handleInitReq(frame *Frame) {
	if err := c.withStateRLock(func() error {
		return nil
	}); err != nil {
		c.connectionError(err)
		return
	}

	var req InitReq
	rbuf := typed.NewReadBuffer(frame.SizedPayload())
	if err := req.read(rbuf); err != nil {
		// TODO(mmihic): Technically probably a protocol error
		c.connectionError(err)
		return
	}

	if req.Version != CurrentProtocolVersion {
		// TODO(mmihic): Send protocol error
		c.connectionError(fmt.Errorf("Unsupported protocol version %d from peer", req.Version))
		return
	}

	c.remotePeerInfo.HostPort = req.InitParams[InitParamHostPort]
	c.remotePeerInfo.ProcessName = req.InitParams[InitParamProcessName]

	res := InitRes{initMessage{id: frame.Header.Id}}
	res.InitParams = InitParams{
		InitParamHostPort:    c.localPeerInfo.HostPort,
		InitParamProcessName: c.localPeerInfo.ProcessName,
	}
	res.Version = CurrentProtocolVersion
	if err := c.sendMessage(&res); err != nil {
		c.connectionError(err)
		return
	}

	c.withStateLock(func() error {
		switch c.state {
		case connectionWaitingToRecvInitReq:
			c.state = connectionActive
		}

		return nil
	})
}

// Handles an incoming InitRes.  If we are waiting for the peer to send us an InitRes, forward the InitRes
// to the waiting goroutine
// TODO(mmihic): There is a race condition here, in that the peer might start sending us requests before
// the goroutine doing initialization has a chance to process the InitRes.  We probably want to move
// the InitRes checking to here (where it will run in the receiver goroutine and thus block new incoming
// messages), and simply signal the init goroutine that we are done
func (c *TChannelConnection) handleInitRes(frame *Frame) {
	if err := c.withStateRLock(func() error {
		switch c.state {
		case connectionWaitingToRecvInitRes:
			return nil
		case connectionClosed, connectionStartClose, connectionInboundClosed:
			return ErrConnectionClosed

		case connectionActive:
			return ErrConnectionAlreadyActive

		case connectionWaitingToSendInitReq:
			return ErrConnectionNotReady

		case connectionWaitingToRecvInitReq:
			return ErrConnectionWaitingOnPeerInit

		default:
			return fmt.Errorf("Connection in unknown state %d", c.state)
		}
	}); err != nil {
		c.connectionError(err)
		return
	}

	c.forwardResFrame(frame)
}

// Sends a standalone message (typically a control message)
func (c *TChannelConnection) sendMessage(msg Message) error {
	f, err := MarshalMessage(msg, c.framePool)
	if err != nil {
		return nil
	}

	select {
	case c.sendCh <- f:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// Receives a standalone message (typically a control message)
func (c *TChannelConnection) recvMessage(ctx context.Context, msg Message, resCh <-chan *Frame) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case frame := <-resCh:
		msgBuf := typed.NewReadBuffer(frame.SizedPayload())
		err := msg.read(msgBuf)
		c.framePool.Release(frame)
		return err
	}
}

// Reserves the next available message id for this connection
func (c *TChannelConnection) NextMessageId() uint32 {
	return atomic.AddUint32(&c.nextMessageId, 1)
}

// Handles a connection error
func (c *TChannelConnection) connectionError(err error) error {
	doClose := false
	c.withStateLock(func() error {
		if c.state != connectionClosed {
			c.state = connectionClosed
			doClose = true
		}
		return nil
	})

	if doClose {
		c.closeNetwork()
	}

	return err
}

// Closes the network connection and all network-related channels
func (c *TChannelConnection) closeNetwork() {
	// NB(mmihic): The sender goroutine	will exit once the connection is closed; no need to close
	// the send channel (and closing the send channel would be dangerous since other goroutine might be sending)
	if err := c.conn.Close(); err != nil {
		c.log.Warning("could not close connection to peer %s: %v", c.remotePeerInfo, err)
	}
}

// Performs an action with the connection state mutex locked
func (c *TChannelConnection) withStateLock(f func() error) error {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()

	return f()
}

// Performs an action with the connection state mutex held in a read lock
func (c *TChannelConnection) withStateRLock(f func() error) error {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()

	return f()
}

// Runs a function with the request map lock held
func (c *TChannelConnection) withReqLock(f func() error) error {
	c.reqMut.Lock()
	defer c.reqMut.Unlock()

	return f()
}

// Main loop that reads frames from the network connection and dispatches to the appropriate handler.
// Run within its own goroutine to prevent overlapping reads on the socket.  Most handlers simply
// send the incoming frame to a channel; the init handlers are a notable exception, since we cannot
// process new frames until the initialization is complete.
func (c *TChannelConnection) readFrames() {
	fhBuf := typed.NewReadBufferWithSize(FrameHeaderSize)

	for {
		if _, err := fhBuf.FillFrom(c.conn, FrameHeaderSize); err != nil {
			c.connectionError(err)
			return
		}

		frame := c.framePool.Get()
		if err := frame.Header.read(fhBuf); err != nil {
			// TODO(mmihic): Should be a protocol error
			c.connectionError(err)
			return
		}

		c.log.Info("Recvd: id=%d:type=%d:sz=%d", frame.Header.Id, frame.Header.Type, frame.Header.Size)

		if _, err := c.conn.Read(frame.SizedPayload()); err != nil {
			c.connectionError(err)
			return
		}

		c.log.Info("Rcvd: %s", hex.EncodeToString(frame.SizedPayload()))

		switch frame.Header.Type {
		case MessageTypeCallReq:
			c.inbound.handleCallReq(frame)
		case MessageTypeCallReqContinue:
			c.inbound.handleCallReqContinue(frame)
		case MessageTypeCallRes:
			c.handleCallRes(frame)
		case MessageTypeCallResContinue:
			c.handleCallResContinue(frame)
		case MessageTypeInitReq:
			c.handleInitReq(frame)
		case MessageTypeInitRes:
			c.handleInitRes(frame)
		case MessageTypeError:
			c.handleError(frame)
		default:
			// TODO(mmihic): Log and close connection with protocol error
		}
	}
}

// Main loop that pulls frames from the send channel and writes them to the connection.
// Run in its own goroutine to prevent overlapping writes on the network socket.
func (c *TChannelConnection) writeFrames() {
	fhBuf := typed.NewWriteBufferWithSize(FrameHeaderSize)
	for f := range c.sendCh {
		fhBuf.Reset()

		c.log.Info("Send: id=%d:type=%d:sz=%d", f.Header.Id, f.Header.Type, f.Header.Size)
		c.log.Info("Send: %s", hex.EncodeToString(f.SizedPayload()))

		if err := f.Header.write(fhBuf); err != nil {
			c.connectionError(NewWriteIOError("frame-header", err))
			return
		}

		if _, err := fhBuf.FlushTo(c.conn); err != nil {
			c.connectionError(NewWriteIOError("frame-header-flush", err))
			return
		}

		if _, err := c.conn.Write(f.SizedPayload()); err != nil {
			c.connectionError(NewWriteIOError("frame-payload", err))
			return
		}

		c.framePool.Release(f)
	}
}

// Creates a new frame around a message
func MarshalMessage(msg Message, pool FramePool) (*Frame, error) {
	f := pool.Get()

	wbuf := typed.NewWriteBuffer(f.Payload[:])
	if err := msg.write(wbuf); err != nil {
		return nil, err
	}

	f.Header.Id = msg.Id()
	f.Header.Type = msg.Type()
	f.Header.Size = uint16(wbuf.BytesWritten())
	return f, nil
}
