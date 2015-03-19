// Copyright (c) 2015 Uber Technologies, Inc.
//
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

'use strict';

var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var PassThrough = require('stream').PassThrough;
var Ready = require('ready-signal');
var TypedError = require('error/typed');

var ArgChunkOutOfOrderError = TypedError({
    type: 'arg-chunk-out-of-order',
    message: 'out of order arg chunk, current: {current} got: {got}',
    current: null,
    got: null
});

var ArgChunkGapError = TypedError({
    type: 'arg-chunk-gap',
    message: 'arg chunk gap, current: {current} got: {got}',
    current: null,
    got: null
});

function ArgStream() {
    var self = this;
    EventEmitter.call(self);
    self.arg1 = StreamArg();
    self.arg2 = StreamArg();
    self.arg3 = StreamArg();

    self.arg1.on('error', passError);
    self.arg2.on('error', passError);
    self.arg3.on('error', passError);
    function passError(err) {
        self.emit('error', err);
    }

    self.arg2.on('start', function onArg2Start() {
        if (!self.arg1._writableState.ended) self.arg1.end();
    });
    self.arg3.on('start', function onArg3Start() {
        if (!self.arg2._writableState.ended) self.arg2.end();
    });
}

inherits(ArgStream, EventEmitter);

function InArgStream() {
    var self = this;
    ArgStream.call(self);

    self.streams = [self.arg1, self.arg2, self.arg3];
    self._iStream = 0;

    self.finished = false;
    var numFinished = 0;
    self.arg1.on('finish', argFinished);
    self.arg2.on('finish', argFinished);
    self.arg3.on('finish', argFinished);
    function argFinished() {
        if (++numFinished === 3) {
            self.finished = true;
            self.emit('finish');
        }
    }
}

inherits(InArgStream, ArgStream);

InArgStream.prototype.handleFrame = function handleFrame(parts) {
    var self = this;

    if (parts === null) {
        while (self._iStream < self.streams.length) {
            self.streams[self._iStream].end();
            self._iStream++;
        }
        return;
    }

    if (self.finished) {
        throw new Error('arg stream finished'); // TODO typed error
    }

    var i = 0;
    var last = null;
    while (i < parts.length && self.streams[self._iStream]) {
        var part = parts[i];
        if (last !== null) advance();
        if (part.length) self.streams[self._iStream].write(part);
        i++;
        last = part;
    }
    if (i < parts.length) {
        throw new Error('frame parts exceeded stream arity'); // TODO clearer / typed error
    }
    function advance() {
        if (self._iStream < self.streams.length) {
            self.streams[self._iStream].end();
            self._iStream++;
        }
    }
};

function OutArgStream() {
    var self = this;
    ArgStream.call(self);

    self.finished = false;
    self.frame = [Buffer(0)];
    self.currentArgN = 1;

    self.arg1.on('finish', function onArg1Finish() {
        handleFrameChunk(1, Buffer(0));
    });
    self.arg2.on('finish', function onArg2Finish() {
        handleFrameChunk(2, Buffer(0));
    });
    self.arg3.on('finish', function onArg3Finish() {
        handleFrameChunk(3, Buffer(0));
        flushParts(true);
        self.finished = true;
        self.emit('finish');
    });

    self.arg1.on('data', function onArg1Data(chunk) {
        handleFrameChunk(1, chunk);
    });
    self.arg2.on('data', function onArg2Data(chunk) {
        handleFrameChunk(2, chunk);
    });
    self.arg3.on('data', function onArg3Data(chunk) {
        handleFrameChunk(3, chunk);
    });

    var immed = null;

    function handleFrameChunk(n, chunk) {
        if (n < self.currentArgN) {
            self.emit('error', ArgChunkOutOfOrderError({
                current: self.currentArgN,
                got: n
            }));
        } else if (n > self.currentArgN) {
            if (n - self.currentArgN > 1) {
                self.emit('error', ArgChunkGapError({
                    current: self.currentArgN,
                    got: n
                }));
            }
            self.currentArgN++;
            self.frame.push(chunk);
        } else if (chunk.length) {
            appendFrameChunk(chunk);
        } else if (self.currentArgN < 3) {
            self.currentArgN++;
            self.frame.push(chunk);
        }
        deferFlushParts();
    }

    function appendFrameChunk(chunk) {
        var i = self.frame.length - 1;
        if (i < 0) {
            self.frame.push(chunk);
        } else {
            var buf = self.frame[i];
            if (buf.length) {
                self.frame[i] = Buffer.concat([buf, chunk]);
            } else {
                self.frame[i] = chunk;
            }
        }
    }

    function deferFlushParts() {
        if (!immed) {
            immed = setImmediate(flushParts);
        }
    }

    function flushParts(isLast) {
        if (immed) {
            clearImmediate(immed);
            immed = null;
        }
        if (self.finished) return;
        isLast = Boolean(isLast);
        var frame = self.frame;
        self.frame = [Buffer(0)];
        if (frame.length) self.emit('frame', frame, isLast);
    }
}

inherits(OutArgStream, ArgStream);

function StreamArg(options) {
    if (!(this instanceof StreamArg)) {
        return new StreamArg(options);
    }
    var self = this;
    PassThrough.call(self, options);
    self.started = false;
    self.onValueReady = self.onValueReady.bind(self);
}
inherits(StreamArg, PassThrough);

StreamArg.prototype._write = function _write(chunk, encoding, callback) {
    var self = this;
    if (!self.started) {
        self.started = true;
        self.emit('start');
    }
    PassThrough.prototype._write.call(self, chunk, encoding, callback);
};

StreamArg.prototype.onValueReady = function onValueReady(callback) {
    var self = this;
    self.onValueReady = Ready();
    bufferStreamData(self, self.onValueReady.signal);
    self.onValueReady(callback);
};

function bufferStreamData(stream, callback) {
    var parts = [];
    stream.on('data', onData);
    stream.on('error', finish);
    stream.on('end', finish);
    function onData(chunk) {
        parts.push(chunk);
    }
    function finish(err) {
        stream.removeListener('data', onData);
        stream.removeListener('error', finish);
        stream.removeListener('end', finish);
        var buf = Buffer.concat(parts);
        if (err === undefined) err = null;
        callback(err, buf);
    }
}

module.exports.InArgStream = InArgStream;
module.exports.OutArgStream = OutArgStream;
