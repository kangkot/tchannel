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

var bufrw = require('bufrw');
var Checksum = require('./checksum');
var ArgsRW = require('./args');
var Frame = require('./frame');

var Flags = {
    Fragment: 0x01
};

// flags:1 csumtype:1 (csum:4){0,1} (arg~2)+
function CallRequestCont(flags, csum, args) {
    if (!(this instanceof CallRequestCont)) {
        return new CallRequestCont(flags, csum, args);
    }
    var self = this;
    self.type = CallRequestCont.TypeCode;
    self.flags = flags || 0;
    if (csum === undefined || csum === null) {
        self.csum = Checksum(Checksum.Types.None);
    } else {
        self.csum = Checksum.objOrType(csum);
    }
    self.args = args || [];
}

CallRequestCont.TypeCode = 0x13;

CallRequestCont.Flags = Flags;

CallRequestCont.RW = bufrw.Struct(CallRequestCont, {
    flags: bufrw.UInt8,      // flags:1
    csum: Checksum.RW,       // csumtype:1 (csum:4){0,1}
    args: ArgsRW(bufrw.buf2) // (arg~2)+
});

CallRequestCont.prototype.splitArgs = function splitArgs(args) {
    var self = this;
    // assert not self.args
    var lenRes = self.constructor.RW.byteLength(self);
    if (lenRes.err) throw lenRes.err;
    var maxBodySize = Frame.MaxBodySize - lenRes.length;
    var remain = maxBodySize;
    var ret = [];
    var isLast = !(self.flags & Flags.Fragment);
    while (args.length) {
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
        if (i >= args.length) args = [];
        self.args = first;
        ret.push(self);
        if (args.length) {
            self = self.constructor(self.flags | Flags.Fragment, self.csum.type);
        }
    }
    if (isLast) ret[ret.length - 1].flags &= ~ Flags.Fragment;
    return ret;
};

CallRequestCont.prototype.updateChecksum = function updateChecksum(prior) {
    var self = this;
    return self.csum.update(self.args, prior);
};

CallRequestCont.prototype.verifyChecksum = function verifyChecksum(prior) {
    var self = this;
    return self.csum.verify(self.args, prior);
};

// flags:1 csumtype:1 (csum:4){0,1} (arg~2)+
function CallResponseCont(flags, csum, args) {
    if (!(this instanceof CallResponseCont)) {
        return new CallResponseCont(flags, csum, args);
    }
    var self = this;
    self.type = CallResponseCont.TypeCode;
    self.flags = flags || 0;
    if (csum === undefined || csum === null) {
        self.csum = Checksum(Checksum.Types.None);
    } else {
        self.csum = Checksum.objOrType(csum);
    }
    self.args = args || [];
}

CallResponseCont.TypeCode = 0x14;

CallResponseCont.Flags = CallRequestCont.Flags;

CallResponseCont.RW = bufrw.Struct(CallResponseCont, {
    flags: bufrw.UInt8,      // flags:1
    csum: Checksum.RW,       // csumtype:1 (csum:4){0},1}
    args: ArgsRW(bufrw.buf2) // (arg~2)+
});

CallResponseCont.prototype.splitArgs = CallRequestCont.prototype.splitArgs;

CallResponseCont.prototype.updateChecksum = function updateChecksum(prior) {
    var self = this;
    return self.csum.update(self.args, prior);
};

CallResponseCont.prototype.verifyChecksum = function verifyChecksum(prior) {
    var self = this;
    return self.csum.verify(self.args, prior);
};

module.exports.RequestCont = CallRequestCont;
module.exports.ResponseCont = CallResponseCont;
