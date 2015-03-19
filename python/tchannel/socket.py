from __future__ import absolute_import

from . import messages
from .messages.types import Types
from .messages.common import PROTOCOL_VERSION
from .exceptions import ProtocolException, InvalidMessageException
from .frame_reader import FrameWriter, FrameReader


class _SocketIOAdapter(object):
    """Represent a ``socket.socket`` instance as a buffer."""
    def __init__(self, connection):
        self._connection = connection

    def read(self, size):
        result = self._connection.recv(size)
        remaining = size - len(result)

        # Ensure that we read as much data as was requested.
        if remaining > 0:
            chunks = [result]
            while remaining > 0:
                s = self._connection.recv(remaining)
                if not s:  # end of stream reached
                    break

                remaining -= len(s)
                chunks.append(s)
            result = "".join(chunks)

        return result

    def write(self, data):
        return self._connection.sendall(data)

    def close(self):
        self._connection.close()


class SocketConnection(object):
    """Adapt a ``socket.socket`` connection as a TChannel connection.

    Use this class to perform synchronous socket operations, e.g. over TCP or a
    Unix Domain Socket.
    """
    def __init__(self, connection):
        self.connection = _SocketIOAdapter(connection)
        self.writer = FrameWriter(self.connection)
        self.reader = FrameReader(self.connection).read()

        self._id_sequence = 0

    def handle_calls(self, handler):
        for call in self.reader:
            handler(call, connection=self)

    def await(self):
        """Decode a full message and return"""
        try:
            ctx = next(self.reader)
        except StopIteration:
            ctx = None
        return ctx

    def next_message_id(self):
        """Generate a new message ID."""
        self._id_sequence += 1
        return self._id_sequence

    def frame_and_write(self, message, message_id=None):
        """Frame and write a message over a connection."""
        if message_id is None:
            message_id = self.next_message_id()
        try:
            self.writer.write(message_id, message)
        except ProtocolException as e:
            raise InvalidMessageException(e.message)
        return message_id

    def ping(self):
        """Send a PING_REQ message to the remote end of the connection."""
        message = messages.PingRequestMessage()
        return self.frame_and_write(message)

    def pong(self, message_id):
        """Reply to a PING_REQ message with a PING_RES."""
        message = messages.PingResponseMessage()
        return self.frame_and_write(message, message_id=message_id)

    def await_handshake(self, headers):
        """Negotiate a common protocol version with a client."""
        ctx = self.await()
        message = ctx.message
        if message.message_type != Types.INIT_REQ:
            raise InvalidMessageException(
                'You need to shake my hand first. Got: %d' %
                message.message_type,
            )
        self.extract_handshake_headers(message)
        response = messages.InitResponseMessage(PROTOCOL_VERSION, headers)
        return self.frame_and_write(response, message_id=ctx.message_id)

    def extract_handshake_headers(self, message):
        """Extract TChannel headers from a handshake."""
        if not message.host_port:
            raise InvalidMessageException('Missing required header: host_port')

        if not message.process_name:
            raise InvalidMessageException(
                'Missing required header: process_name'
            )

        self.remote_host = message.host_port
        self.remote_process_name = message.process_name
        self.requested_version = message.version

    def initiate_handshake(self, headers):
        """Send a handshake offer to a server."""
        message = messages.InitRequestMessage(
            version=PROTOCOL_VERSION,
            headers=headers
        )
        self.handshake_headers = headers
        return self.frame_and_write(message)

    def await_handshake_reply(self):
        context = self.await()
        message = context.message
        if message.message_type != Types.INIT_RES:
            raise InvalidMessageException(
                'Expected handshake response, got %d' %
                message.message_type,
            )

        self.extract_handshake_headers(message)
        return message
