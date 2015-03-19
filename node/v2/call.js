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

var bufrw = require('bufrw');
var Checksum = require('./checksum');
var header = require('./header');
var ArgsRW = require('./args');
var Frame = require('./frame');

var Flags = {
    Fragment: 0x01
};

module.exports.Request = CallRequest;
module.exports.Response = CallResponse;

var emptyTracing = new Buffer(25);
emptyTracing.fill(0);

// TODO: validate transport header names?
// TODO: Checksum-like class for tracing

/* jshint maxparams:10 */

// flags:1 ttl:4 tracing:24 traceflags:1 service~1 nh:1 (hk~1 hv~1){nh} csumtype:1 (csum:4){0,1} (arg~2)+
function CallRequest(flags, ttl, tracing, service, headers, csum, args) {
    if (!(this instanceof CallRequest)) {
        return new CallRequest(flags, ttl, tracing, service, headers, csum, args);
    }
    var self = this;
    self.type = CallRequest.TypeCode;
    self.flags = flags || 0;
    self.ttl = ttl || 0;
    self.tracing = tracing || emptyTracing;
    self.service = service || '';
    self.headers = headers || {};
    if (csum === undefined || csum === null) {
        self.csum = Checksum(Checksum.Types.None);
    } else {
        self.csum = Checksum.objOrType(csum);
    }
    self.args = args || [];
}

CallRequest.Cont = require('./cont').CallRequestCont;

CallRequest.TypeCode = 0x03;

CallRequest.Flags = Flags;

CallRequest.RW = bufrw.Struct(CallRequest, [
    {name: 'flags', rw: bufrw.UInt8},            // flags:1
    {name: 'ttl', rw: bufrw.UInt32BE},           // ttl:4
    {name: 'tracing', rw: bufrw.FixedWidth(25)}, // tracing:24 traceflags:1
    {name: 'service', rw: bufrw.str1},           // service~1
    {name: 'headers', rw: header.header1},       // nh:1 (hk~1 hv~1){nh}
    {name: 'csum', rw: Checksum.RW},             // csumtype:1 (csum:4){0,1}
    {name: 'args', rw: ArgsRW(bufrw.buf2)},      // (arg~2)+
]);

CallRequest.prototype.splitArgs = function splitArgs(args) {
    var self = this;
    // assert not self.args
    var lenRes = self.constructor.RW.byteLength(self);
    if (lenRes.err) throw lenRes.err;
    var maxBodySize = Frame.MaxBodySize - lenRes.length;
    var remain = maxBodySize;
    var first = [];
    var argSize = 2;
    for (var i = 0; i < args.length; i++) {
        var argLength = argSize + args[i].length;
        if (argLength < remain) {
            first.push(args[i]);
            remain -= argLength;
        } else {
            first.push(args[i].slice(0, remain - argSize));
            args = [args[i].slice(remain - argSize)].concat(args.slice(i));
            break;
        }
    }
    self.args = first;
    if (args.length) {
        var isLast = !(self.flags & CallRequest.Flags.Fragment);
        var cont = self.constructor.Cont(self.flags | CallRequest.Flags.Fragment, self.csum.type);
        var ret = cont.splitArgs(args);
        ret.unshift(self);
        if (isLast) {
            ret[ret.length - 1].flags &= ~ CallRequest.Flags.Fragment;
        }
        return ret;
    } else {
        return [self];
    }
};

CallRequest.prototype.updateChecksum = function updateChecksum() {
    var self = this;
    return self.csum.update(self.args);
};

CallRequest.prototype.verifyChecksum = function verifyChecksum() {
    var self = this;
    return self.csum.verify(self.args);
};

// flags:1 code:1 tracing:24 traceflags:1 nh:1 (hk~1 hv~1){nh} csumtype:1 (csum:4){0,1} (arg~2)+
function CallResponse(flags, code, tracing, headers, csum, args) {
    if (!(this instanceof CallResponse)) {
        return new CallResponse(flags, code, tracing, headers, csum, args);
    }
    var self = this;
    self.type = CallResponse.TypeCode;
    self.flags = flags || 0;
    self.code = code || CallResponse.Codes.OK;
    self.tracing = tracing || emptyTracing;
    self.headers = headers || {};
    if (csum === undefined || csum === null) {
        self.csum = Checksum(Checksum.Types.None);
    } else {
        self.csum = Checksum.objOrType(csum);
    }
    self.args = args || [];
}

CallResponse.Cont = require('./cont').CallResponseCont;

CallResponse.TypeCode = 0x04;

CallResponse.Flags = CallRequest.Flags;

CallResponse.Codes = {
    OK: 0x00,
    Error: 0x01
};

CallResponse.RW = bufrw.Struct(CallResponse, [
    {name: 'flags', rw: bufrw.UInt8},            // flags:1
    {name: 'code', rw: bufrw.UInt8},             // code:1
    {name: 'tracing', rw: bufrw.FixedWidth(25)}, // tracing:24 traceflags:1
    {name: 'headers', rw: header.header1},       // nh:1 (hk~1 hv~1){nh}
    {name: 'csum', rw: Checksum.RW},             // csumtype:1 (csum:4){0},1}
    {name: 'args', rw: ArgsRW(bufrw.buf2)},      // (arg~2)+
]);

CallResponse.prototype.splitArgs = CallRequest.prototype.splitArgs;

CallResponse.prototype.updateChecksum = function updateChecksum() {
    var self = this;
    return self.csum.update(self.args);
};

CallResponse.prototype.verifyChecksum = function verifyChecksum() {
    var self = this;
    return self.csum.verify(self.args);
};
