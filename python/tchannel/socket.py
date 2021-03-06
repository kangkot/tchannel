from __future__ import absolute_import

import socket

from .frame_reader import FrameReader
from .connection import Connection


class _SocketIOAdapter(object):
    """Represent a ``socket.socket`` instance as a buffer."""
    def __init__(self, connection):
        self._connection = connection

    def read(self, size, callback=None):
        assert not callback, 'async not supported for sockets'
        result = self._connection.recv(size)
        remaining = size - len(result)

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

    def write(self, data, callback=None):
        assert not callback, 'async not supported for sockets'
        return self._connection.sendall(data)


class SocketConnection(Connection):
    """Adapt a ``socket.socket`` connection as a TChannel connection.

    Use this class to perform synchronous socket operations, e.g. over TCP or a
    Unix Domain Socket.
    """
    def __init__(self, connection):
        adapted = _SocketIOAdapter(connection)
        super(SocketConnection, self).__init__(adapted)

        self.reader = FrameReader(adapted).read()

    def handle_calls(self, handler):
        for call in self.reader:
            handler(call, connection=self)

    def await(self, callback):
        """Decode a full message and return"""
        callback(next(self.reader))

    def next(self):
        return next(self.reader)

    @classmethod
    def outgoing(cls, hostport):
        host, port = hostport.rsplit(":", 1)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, int(port)))

        conn = cls(sock)

        conn.initiate_handshake(headers={
            'host_port': '%s:%s' % (host, port),
            'process_name': 'tchannel_client-%s' % port
        })
        conn.await_handshake_reply()
        return conn

    def close(self):
        return self._connection._connection.close()

    # Python 3. Yay.
    __next__ = next
