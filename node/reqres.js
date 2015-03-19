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

'use strict';

var inherits = require('util').inherits;
var parallel = require('run-parallel');

var InArgStream = require('./argstream').InArgStream;
var OutArgStream = require('./argstream').OutArgStream;

var emptyTracing = Buffer(25); // TODO: proper tracing object

var States = Object.create(null);
States.Initial = 0;
States.Streaming = 1;
States.Done = 2;

function TChannelIncomingRequest(id, options) {
    if (!(this instanceof TChannelIncomingRequest)) {
        return new TChannelIncomingRequest(id, options);
    }
    options = options || {};
    var self = this;
    InArgStream.call(self);
    self.state = States.Initial;
    self.id = id || 0;
    self.ttl = options.ttl || 0;
    self.tracing = options.tracing || emptyTracing;
    self.service = options.service || '';
    self.remoteAddr = null;
    self.headers = options.headers || {};
    self.checksum = options.checksum || null;
    self.on('finish', function onFinish() {
        self.state = States.Done;
    });
}

inherits(TChannelIncomingRequest, InArgStream);

TChannelIncomingRequest.prototype.finish = function finish() {
    var self = this;
    self.state = States.Done;
};

function TChannelIncomingResponse(id, options) {
    if (!(this instanceof TChannelIncomingResponse)) {
        return new TChannelIncomingResponse(id, options);
    }
    options = options || {};
    var self = this;
    InArgStream.call(self);
    self.state = States.Initial;
    self.id = id || 0;
    self.code = options.code || 0;
    self.checksum = options.checksum || null;
    self.ok = self.code === 0; // TODO: probably okay, but a bit jank
    self.on('finish', function onFinish() {
        self.state = States.Done;
    });
}

inherits(TChannelIncomingResponse, InArgStream);

TChannelIncomingResponse.prototype.finish = function finish() {
    var self = this;
    self.state = States.Done;
};

function TChannelOutgoingRequest(id, options) {
    if (!(this instanceof TChannelOutgoingRequest)) {
        return new TChannelOutgoingRequest(id, options);
    }
    options = options || {};
    if (!options.sendFrame) {
        throw new Error('missing sendFrame');
    }
    var self = this;
    OutArgStream.call(self);
    self.state = States.Initial;
    self.id = id || 0;
    self.ttl = options.ttl || 0;
    self.tracing = options.tracing || emptyTracing;
    self.service = options.service || '';
    self.headers = options.headers || {};
    self.checksumType = options.checksumType || 0;
    self.checksum = options.checksum || null;
    self.sendFrame = options.sendFrame;
    self.on('frame', function onFrame(parts, isLast) {
        switch (self.state) {
            case States.Initial:
                self.sendCallRequestFrame(isLast, parts);
                break;
            case States.Streaming:
                self.sendCallRequestContFrame(isLast, parts);
                break;
            case States.Done:
                // TODO: could probably happen normally, like say if a
                // streaming request is canceled
                throw new Error('got frame in done state'); // TODO: typed error
        }
    });
    self.on('finish', function onFinish() {
        // TODO: should be redundant with self.sendCallRequest(Cont)Frame
        // having been called with isLast=true
        self.state = States.Done;
    });
}

inherits(TChannelOutgoingRequest, OutArgStream);

TChannelOutgoingRequest.prototype.sendCallRequestFrame = function sendCallRequestFrame(isLast, args) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            self.sendFrame.callRequest(isLast, args);
            if (isLast) {
                self.state = States.Done;
            } else {
                self.state = States.Streaming;
            }
            break;
        case States.Streaming:
            throw new Error('first request frame already sent'); // TODO: typed error
        case States.Done:
            throw new Error('request already done'); // TODO: typed error
    }
};

TChannelOutgoingRequest.prototype.sendCallRequestContFrame = function sendCallRequestContFrame(isLast, args) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            throw new Error('first request frame not sent'); // TODO: typed error
        case States.Streaming:
            self.sendFrame.callRequestCont(isLast, args);
            if (isLast) {
                self.state = States.Done;
            }
            break;
        case States.Done:
            throw new Error('request already done'); // TODO: typed error
    }
};

TChannelOutgoingRequest.prototype.send = function send(arg1, arg2, arg3, callback) {
    var self = this;
    if (callback) self.hookupCallback(callback);
    self.arg1.end(arg1);
    self.arg2.end(arg2);
    self.arg3.end(arg3);
    return self;
};

TChannelOutgoingRequest.prototype.hookupCallback = function hookupCallback(callback) {
    var self = this;
    self.once('error', onError);
    self.once('response', onResponse);
    function onError(err) {
        self.removeListener('response', onResponse);
        callback(err, null);
    }
    function onResponse(res) {
        self.removeListener('error', onError);
        if (callback.canStream) {
            callback(null, res);
        } else {
            parallel({
                arg2: res.arg2.onValueReady,
                arg3: res.arg3.onValueReady
            }, function argsDone(err, args) {
                callback(err, res, args.arg2, args.arg3);
            });
        }
    }
    return self;
};

function TChannelOutgoingResponse(id, options) {
    if (!(this instanceof TChannelOutgoingResponse)) {
        return new TChannelOutgoingResponse(id, options);
    }
    options = options || {};
    if (!options.sendFrame) {
        throw new Error('missing sendFrame');
    }
    var self = this;
    OutArgStream.call(self);
    self.state = States.Initial;
    self.id = id || 0;
    self.code = options.code || 0;
    self.tracing = options.tracing || emptyTracing;
    self.headers = options.headers || {};
    self.checksumType = options.checksumType || 0;
    self.checksum = options.checksum || null;
    self.ok = true;
    self.sendFrame = options.sendFrame;
    self.on('frame', function onFrame(parts, isLast) {
        switch (self.state) {
            case States.Initial:
                self.sendCallResponseFrame(isLast, parts);
                break;
            case States.Streaming:
                self.sendCallResponseContFrame(isLast, parts);
                break;
            case States.Done:
                // TODO: could happen easily if an error frame is sent
                // mid-stream causing a transition to Done
                throw new Error('got frame in done state'); // TODO: typed error
        }
    });
    self.on('finish', function onFinish() {
        self.state = States.Done;
    });
}

inherits(TChannelOutgoingResponse, OutArgStream);

TChannelOutgoingResponse.prototype.sendCallResponseFrame = function sendCallResponseFrame(isLast, args) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            self.sendFrame.callResponse(isLast, args);
            if (isLast) {
                self.state = States.Done;
            } else {
                self.state = States.Streaming;
            }
            break;
        case States.Streaming:
            throw new Error('first response frame already sent'); // TODO: typed error
        case States.Done:
            throw new Error('response already done'); // TODO: typed error
    }
};

TChannelOutgoingResponse.prototype.sendCallResponseContFrame = function sendCallResponseContFrame(isLast, args) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            throw new Error('first response frame not sent'); // TODO: typed error
        case States.Streaming:
            self.sendFrame.callResponseCont(isLast, args);
            if (isLast) {
                self.state = States.Done;
            } else {
                self.state = States.Streaming;
            }
            break;
        case States.Done:
            throw new Error('response already done'); // TODO: typed error
    }
};

TChannelOutgoingResponse.prototype.sendErrorFrame = function sendErrorFrame(codeString, message) {
    var self = this;
    // TODO: is it okay to send an error frame mid stream? should we then send
    // a last cont frame?
    if (self.state === States.Done) {
        throw new Error('response already done');
    } else {
        self.sendFrame.error(codeString, message);
        self.state = States.Done;
    }
};

TChannelOutgoingResponse.prototype.setOK = function setOK(ok) {
    var self = this;
    if (self.state !== States.Initial) {
        throw new Error('response already started'); // TODO typed error
    }
    self.ok = ok;
    self.code = ok ? 0 : 1; // TODO: too coupled to v2 specifics?
};

TChannelOutgoingResponse.prototype.sendOk = function sendOk(res1, res2) {
    var self = this;
    self.setOK(true);
    self.arg2.end(res1);
    self.arg3.end(res2);
};

TChannelOutgoingResponse.prototype.sendNotOk = function sendNotOk(res1, res2) {
    var self = this;
    self.setOK(false);
    self.arg2.end(res1);
    self.arg3.end(res2);
};

module.exports.States = States;
module.exports.IncomingRequest = TChannelIncomingRequest;
module.exports.IncomingResponse = TChannelIncomingResponse;
module.exports.OutgoingRequest = TChannelOutgoingRequest;
module.exports.OutgoingResponse = TChannelOutgoingResponse;
