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

var TypedError = require('error/typed');
var Duplex = require('stream').Duplex;
var util = require('util');

var reqres = require('../reqres');
var TChannelOutgoingRequest = reqres.OutgoingRequest;
var TChannelOutgoingResponse = reqres.OutgoingResponse;
var TChannelIncomingRequest = reqres.IncomingRequest;
var TChannelIncomingResponse = reqres.IncomingResponse;
var v2 = require('./index');

module.exports = TChannelV2Handler;

var TChannelUnhandledFrameTypeError = TypedError({
    type: 'tchannel.unhandled-frame-type',
    message: 'unhandled frame type {typeCode}',
    typeCode: null
});

var InvalidCodeStringError = TypedError({
    type: 'tchannel.invalid-code-string',
    message: 'Invalid Error frame code: {codeString}',
    codeString: null
});

function TChannelV2Handler(channel, options) {
    if (!(this instanceof TChannelV2Handler)) {
        return new TChannelV2Handler(channel, options);
    }
    var self = this;
    Duplex.call(self, {
        objectMode: true
    });
    self.channel = channel;
    self.remoteHostPort = null; // filled in by identify message
    self.lastSentFrameId = 0;
    // TODO: GC these... maybe that's up to TChannel itself wrt ops
    self.streamingReq = Object.create(null);
    self.streamingRes = Object.create(null);
}

util.inherits(TChannelV2Handler, Duplex);

TChannelV2Handler.prototype.nextFrameId = function nextFrameId() {
    var self = this;
    self.lastSentFrameId = (self.lastSentFrameId + 1) % v2.Frame.MaxId;
    return self.lastSentFrameId;
};

TChannelV2Handler.prototype._write = function _write(frame, encoding, callback) {
    var self = this;
    switch (frame.body.type) {
        case v2.Types.InitRequest:
            return self.handleInitRequest(frame, callback);
        case v2.Types.InitResponse:
            return self.handleInitResponse(frame, callback);
        case v2.Types.CallRequest:
            return self.handleCallRequest(frame, callback);
        case v2.Types.CallResponse:
            return self.handleCallResponse(frame, callback);
        case v2.Types.CallRequestCont:
            return self.handleCallRequestCont(frame, callback);
        case v2.Types.CallResponseCont:
            return self.handleCallResponseCont(frame, callback);
        case v2.Types.ErrorResponse:
            return self.handleError(frame, callback);
        default:
            return callback(TChannelUnhandledFrameTypeError({
                typeCode: frame.body.type
            }));
    }
};

TChannelV2Handler.prototype._read = function _read(/* n */) {
    /* noop */
};

TChannelV2Handler.prototype.handleInitRequest = function handleInitRequest(reqFrame, callback) {
    var self = this;
    if (self.remoteHostPort !== null) {
        return callback(new Error('duplicate init request')); // TODO typed error
    }
    /* jshint camelcase:false */
    var headers = reqFrame.body.headers;
    var init = {
        hostPort: headers.host_port,
        processName: headers.process_name
    };
    /* jshint camelcase:true */
    self.remoteHostPort = init.hostPort;
    self.emit('init.request', init);
    self.sendInitResponse(reqFrame);
    callback();
};

TChannelV2Handler.prototype.handleInitResponse = function handleInitResponse(resFrame, callback) {
    var self = this;
    if (self.remoteHostPort !== null) {
        return callback(new Error('duplicate init response')); // TODO typed error
    }
    /* jshint camelcase:false */
    var headers = resFrame.body.headers;
    var init = {
        hostPort: headers.host_port,
        processName: headers.process_name
    };
    /* jshint camelcase:true */
    self.remoteHostPort = init.hostPort;
    self.emit('init.response', init);
    callback();
};

TChannelV2Handler.prototype.handleCallRequest = function handleCallRequest(reqFrame, callback) {
    var self = this;
    if (self.remoteHostPort === null) {
        return callback(new Error('call request before init request')); // TODO typed error
    }
    var err = reqFrame.body.verifyChecksum();
    if (err) {
        callback(err); // TODO wrap context
        return;
    }
    var req = self.buildIncomingRequest(reqFrame);
    req.checksum = reqFrame.body.csum;
    if (req.state === reqres.States.Streaming) {
        self.streamingReq[req.id] = req;
    }
    self.emit('call.incoming.request', req);
    callback();
};

TChannelV2Handler.prototype.handleCallResponse = function handleCallResponse(resFrame, callback) {
    var self = this;
    if (self.remoteHostPort === null) {
        return callback(new Error('call response before init response')); // TODO typed error
    }
    var err = resFrame.body.verifyChecksum();
    if (err) {
        callback(err); // TODO wrap context
        return;
    }
    var res = self.buildIncomingResponse(resFrame);
    res.checksum = resFrame.body.csum;
    if (res.state === reqres.States.Streaming) {
        self.streamingRes[res.id] = res;
    }
    self.emit('call.incoming.response', res);
    callback();
};

TChannelV2Handler.prototype.handleCallRequestCont = function handleCallRequestCont(reqFrame, callback) {
    var self = this;
    if (self.remoteHostPort === null) {
        return callback(new Error('call request cont before init request')); // TODO typed error
    }
    var id = reqFrame.id;
    var req = self.streamingReq[id];
    if (!req) {
        return callback(new Error('call request cont for unknown request')); // TODO typed error
    }

    var csum = req.checksum;
    if (csum.type !== reqFrame.body.csum.type) {
        callback(new Error('checksum type changed mid-tream')); // TODO typed error
        return;
    }
    var err = reqFrame.body.verifyChecksum(csum.val);
    if (err) {
        callback(err); // TODO wrap context
        return;
    }
    req.checksum = reqFrame.body.csum;

    switch (req.state) {
        case reqres.States.Initial:
            callback(new Error('got cont to initial req')); // TODO typed error
            break;
        case reqres.States.Streaming:
            self.continueStream(req, reqFrame, callback);
            break;
        case reqres.States.Done:
            callback(new Error('got cont to done req')); // TODO typed error
            break;
    }
};

TChannelV2Handler.prototype.handleCallResponseCont = function handleCallResponseCont(resFrame, callback) {
    var self = this;
    if (self.remoteHostPort === null) {
        return callback(new Error('call response cont before init response')); // TODO typed error
    }
    var id = resFrame.id;
    var res = self.streamingRes[id];
    if (!res) {
        return callback(new Error('call response cont for unknown response')); // TODO typed error
    }

    var csum = res.checksum;
    if (csum.type !== resFrame.body.csum.type) {
        callback(new Error('checksum type changed mid-tream')); // TODO typed error
        return;
    }
    var err = resFrame.body.verifyChecksum(csum.val);
    if (err) {
        callback(err); // TODO wrap context
        return;
    }
    res.checksum = resFrame.body.csum;

    switch (res.state) {
        case reqres.States.Initial:
            callback(new Error('got cont to initial res')); // TODO typed error
            break;
        case reqres.States.Streaming:
            self.continueStream(res, resFrame, callback);
            break;
        case reqres.States.Done:
            callback(new Error('got cont to done req')); // TODO typed error
            break;
    }
};

TChannelV2Handler.prototype.handleError = function handleError(errFrame, callback) {
    var self = this;

    var id = errFrame.id;
    var code = errFrame.body.code;
    var message = String(errFrame.body.message);
    var err = v2.ErrorResponse.CodeErrors[code]({
        originalId: id,
        message: message
    });
    if (id === v2.Frame.NullId) {
        // fatal error not associated with a prior frame
        callback(err);
    } else {
        self.emit('call.incoming.error', err);
        callback();
    }
};

TChannelV2Handler.prototype.continueStream = function continueStream(r, frame, callback) {
    r.handleFrame(frame.body.args);
    if (!(frame.body.flags & v2.CallRequest.Flags.Fragment)) {
        r.handleFrame(null);
    }
    callback();
};

TChannelV2Handler.prototype.sendInitRequest = function sendInitRequest() {
    var self = this;
    var id = self.nextFrameId(); // TODO: assert(id === 1)?
    var hostPort = self.channel.hostPort;
    var processName = self.channel.processName;
    var body = v2.InitRequest(v2.VERSION, {
        /* jshint camelcase:false */
        host_port: hostPort,
        process_name: processName
        /* jshint camelcase:true */
    });
    var reqFrame = v2.Frame(id, body);
    self.push(reqFrame);
};

TChannelV2Handler.prototype.sendInitResponse = function sendInitResponse(reqFrame) {
    var self = this;
    var id = reqFrame.id;
    var hostPort = self.channel.hostPort;
    var processName = self.channel.processName;
    var body = v2.InitResponse(v2.VERSION, {
        /* jshint camelcase:false */
        host_port: hostPort,
        process_name: processName
        /* jshint camelcase:true */
    });
    var resFrame = v2.Frame(id, body);
    self.push(resFrame);
};

TChannelV2Handler.prototype.sendCallRequestFrame = function sendCallRequestFrame(req, flags, args) {
    var self = this;
    var reqBody = v2.CallRequest(
        flags, req.ttl, req.tracing,
        req.service, req.headers,
        req.checksumType);
    var csum = req.checksum = reqBody.csum;
    var bodies = reqBody.splitArgs(args);
    for (var i = 0; i < bodies.length; i++) {
        var body = bodies[i];
        if (i === 0) {
            body.updateChecksum();
        } else {
            body.updateChecksum(csum.val);
        }
        csum = req.checksum = body.csum;
        self.push(v2.Frame(req.id, body));
    }
};

TChannelV2Handler.prototype.sendCallResponseFrame = function sendCallResponseFrame(res, flags, args) {
    var self = this;
    var code = res.ok ? v2.CallResponse.Codes.OK : v2.CallResponse.Codes.Error;
    var resBody = v2.CallResponse(
        flags, code, res.tracing,
        res.headers, res.checksumType);
    var csum = res.checksum = resBody.csum;
    var bodies = resBody.splitArgs(args);
    for (var i = 0; i < bodies.length; i++) {
        var body = bodies[i];
        if (i === 0) {
            body.updateChecksum();
        } else {
            body.updateChecksum(csum.val);
        }
        csum = res.checksum = body.csum;
        self.push(v2.Frame(res.id, body));
    }
};

TChannelV2Handler.prototype.sendCallRequestContFrame = function sendCallRequestContFrame(req, flags, args) {
    var self = this;
    var csum = req.checksum;
    var reqBody = v2.CallRequestCont(flags, csum.type);
    var bodies = reqBody.splitArgs(args);
    for (var i = 0; i < bodies.length; i++) {
        var body = bodies[i];
        body.updateChecksum(csum.val);
        csum = req.checksum = body.csum;
        self.push(v2.Frame(req.id, body));
    }
};

TChannelV2Handler.prototype.sendCallResponseContFrame = function sendCallResponseContFrame(res, flags, args) {
    // TODO: refactor this all the way back out through the op handler calling convention
    var self = this;
    var csum = res.checksum;
    var resBody = v2.CallResponseCont(flags, csum.type);
    var bodies = resBody.splitArgs(args);
    for (var i = 0; i < bodies.length; i++) {
        var body = bodies[i];
        body.updateChecksum(csum.val);
        csum = res.checksum = body.csum;
        self.push(v2.Frame(res.id, body));
    }
};

TChannelV2Handler.prototype.sendErrorFrame = function sendErrorFrame(req, codeString, message) {
    var self = this;

    var code = v2.ErrorResponse.Codes[codeString];
    if (code === undefined) {
        throw InvalidCodeStringError({
            codeString: codeString
        });
    }

    var errBody = v2.ErrorResponse(code, req.id, message);
    var errFrame = v2.Frame(req.id, errBody);
    self.push(errFrame);
};

TChannelV2Handler.prototype.buildOutgoingRequest = function buildOutgoingRequest(options) {
    var self = this;
    var id = self.nextFrameId();
    if (options.checksumType === undefined || options.checksumType === null) {
        options.checksumType = v2.Checksum.Types.FarmHash32;
    }
    options.sendFrame = {
        callRequest: sendCallRequestFrame,
        callRequestCont: sendCallRequestContFrame
    };
    var req = TChannelOutgoingRequest(id, options);
    return req;

    function sendCallRequestFrame(isLast, args) {
        var flags = 0;
        if (!isLast) flags |= v2.CallResponse.Flags.Fragment;
        self.sendCallRequestFrame(req, flags, args);
    }

    function sendCallRequestContFrame(isLast, args) {
        var flags = 0;
        if (!isLast) flags |= v2.CallResponse.Flags.Fragment;
        self.sendCallRequestContFrame(req, flags, args);
    }
};

TChannelV2Handler.prototype.buildOutgoingResponse = function buildOutgoingResponse(req) {
    var self = this;
    var res = TChannelOutgoingResponse(req.id, {
        tracing: req.tracing,
        headers: {},
        checksumType: req.checksumType,
        sendFrame: {
            callResponse: sendCallResponseFrame,
            callResponseCont: sendCallResponseContFrame,
            error: sendErrorFrame
        }
    });
    // TODO: if we really need this, then we need to buffer / keep the arg1
    // value on TChannelIncomingRequest
    //     res.arg1.end(req.arg1Value);
    res.arg1.end();
    return res;

    function sendCallResponseFrame(isLast, args) {
        var flags = 0;
        if (!isLast) flags |= v2.CallResponse.Flags.Fragment;
        self.sendCallResponseFrame(res, flags, args);
    }

    function sendCallResponseContFrame(isLast, args) {
        var flags = 0;
        if (!isLast) flags |= v2.CallResponse.Flags.Fragment;
        self.sendCallResponseContFrame(res, flags, args);
    }

    function sendErrorFrame(codeString, message) {
        self.sendErrorFrame(req, codeString, message);
    }
};

TChannelV2Handler.prototype.buildIncomingRequest = function buildIncomingRequest(reqFrame) {
    var req = TChannelIncomingRequest(reqFrame.id, {
        id: reqFrame.id,
        ttl: reqFrame.ttl,
        tracing: reqFrame.tracing,
        service: reqFrame.service,
        headers: reqFrame.headers,
        checksum: reqFrame.body.csum
    });
    req.handleFrame(reqFrame.body.args);
    if (reqFrame.body.flags & v2.CallRequest.Flags.Fragment) {
        req.state = reqres.States.Streaming;
    } else {
        req.handleFrame(null);
    }
    return req;
};

TChannelV2Handler.prototype.buildIncomingResponse = function buildIncomingResponse(resFrame) {
    var res = TChannelIncomingResponse(resFrame.id, {
        code: resFrame.body.code,
        checksum: resFrame.body.csum
    });
    res.handleFrame(resFrame.body.args);
    if (resFrame.body.flags & v2.CallRequest.Flags.Fragment) {
        res.state = reqres.States.Streaming;
    } else {
        res.handleFrame(null);
    }
    return res;
};
