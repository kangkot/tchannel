package tchannel

// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import (
	"golang.org/x/net/context"
	"net"
	"time"
)

// A Handler is an object hat can be registered with a Channel
// to process incoming calls for a given service and operation
type Handler interface {
	// Handles an incoming call for service
	Handle(ctx context.Context, call *InboundCall)
}

// The HandlerFunc is an adapter to allow the use of ordering functions as
// TChannel handlers.  If f is a function with the appropriate signature,
// HandlerFunc(f) is a Hander object that calls f
type HandlerFunc func(ctx context.Context, call *InboundCall)

// Handle calls f(ctx, call)
func (f HandlerFunc) Handle(ctx context.Context, call *InboundCall) { f(ctx, call) }

// ChannelOptions are used to control parameters on a create a TChannel
type ChannelOptions struct {
	// Default Connection options
	DefaultConnectionOptions ConnectionOptions

	// The name of the process, for logging and reporting to peers
	ProcessName string

	// The logger to use for this channel
	Logger Logger
}

// A TChannel is a bi-directional connection to the peering and routing network.  Applications
// can use a TChannel to make service calls to remote peers via BeginCall, or to listen for incoming calls
// from peers.  Once the channel is created, applications should call the ListenAndHandle method to
// listen for incoming peer connections.  Because channels are bi-directional, applications should call
// ListenAndHandle even if they do not offer any services
type TChannel struct {
	log               Logger
	hostPort          string
	processName       string
	connectionOptions ConnectionOptions
	handlers          handlerMap
	l                 net.Listener
}

// NewChannel creates a new Channel that will bind to the given host and port.  If no port is provided,
// the channel will start on an OS assigned port
func NewChannel(hostPort string, opts *ChannelOptions) (*TChannel, error) {
	if opts == nil {
		opts = &ChannelOptions{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = NullLogger{}
	}

	ch := &TChannel{
		connectionOptions: opts.DefaultConnectionOptions,
		processName:       opts.ProcessName,
		log:               logger,
	}

	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		ch.log.Errorf("Could not resolve network %s: %v", hostPort, err)
		return nil, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		ch.log.Errorf("Could not listen on %s: %v", hostPort, err)
		return nil, err
	}

	ch.l = l
	ch.hostPort = l.Addr().String()
	ch.connectionOptions.PeerInfo.HostPort = ch.hostPort
	ch.connectionOptions.PeerInfo.ProcessName = ch.processName
	ch.log.Infof("%s listening on %s", ch.processName, ch.hostPort)
	return ch, nil
}

// HostPort returns the host and port on which the Channel is listening
func (ch *TChannel) HostPort() string {
	return ch.hostPort
}

// Register regsters a handler for a service+operation pair
func (ch *TChannel) Register(h Handler, serviceName, operationName string) {
	ch.handlers.register(h, serviceName, operationName)
}

// BeginCall starts a new call to a remote peer, returning an OutboundCall that can
// be used to write the arguments of the call
// TODO(mmihic): Support CallOptions such as format, request specific checksums, retries, etc
func (ch *TChannel) BeginCall(ctx context.Context, hostPort,
	serviceName, operationName string) (*OutboundCall, error) {
	// TODO(mmihic): Keep-alive, manage pools, use existing inbound if possible, all that jazz
	nconn, err := net.Dial("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	conn, err := newOutboundConnection(ch, nconn, &ch.connectionOptions)
	if err != nil {
		return nil, err
	}

	if err := conn.sendInit(ctx); err != nil {
		return nil, err
	}

	call, err := conn.beginCall(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	if err := call.writeOperation([]byte(operationName)); err != nil {
		return nil, err
	}

	return call, nil
}

// RoundTrip calls a peer and waits for the response
func (ch *TChannel) RoundTrip(ctx context.Context, hostPort, serviceName, operationName string,
	reqArg2, reqArg3 Output, resArg2, resArg3 Input) (bool, error) {

	call, err := ch.BeginCall(ctx, hostPort, serviceName, operationName)
	if err != nil {
		return false, err
	}

	if err := call.WriteArg2(reqArg2); err != nil {
		return false, err
	}

	if err := call.WriteArg3(reqArg3); err != nil {
		return false, err
	}

	if err := call.Response().ReadArg2(resArg2); err != nil {
		return false, err
	}

	if err := call.Response().ReadArg3(resArg3); err != nil {
		return false, err
	}

	return call.Response().ApplicationError(), nil
}

// ListenAndHandle runs a listener to accept and manage new incoming connections.
// Blocks until the channel is closed.
func (ch *TChannel) ListenAndHandle() error {
	acceptBackoff := 0 * time.Millisecond

	for {
		netConn, err := ch.l.Accept()
		if err != nil {
			// Backoff from new accepts if this is a temporary error
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if acceptBackoff == 0 {
					acceptBackoff = 5 * time.Millisecond
				} else {
					acceptBackoff *= 2
				}
				if max := 1 * time.Second; acceptBackoff > max {
					acceptBackoff = max
				}
				ch.log.Warnf("accept error: %v; retrying in %v", err, acceptBackoff)
				time.Sleep(acceptBackoff)
				continue
			} else {
				ch.log.Errorf("unrecoverable accept error: %v; closing server", err)
				return nil
			}
		}

		acceptBackoff = 0

		_, err = newInboundConnection(ch, netConn, &ch.connectionOptions)
		if err != nil {
			// Server is getting overloaded - begin rejecting new connections
			ch.log.Errorf("could not create new TChannelConnection for incoming conn: %v", err)
			netConn.Close()
			continue
		}

		// TODO(mmihic): Register connection so we can close them when the channel is closed
	}
}
