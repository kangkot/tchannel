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

var parallel = require('run-parallel');
var Buffer = require('buffer').Buffer;
var allocCluster = require('./lib/alloc-cluster.js');
var EndpointHandler = require('../endpoint-handler.js');

allocCluster.test('register() with different results', 2, function t(cluster, assert) {
    var one = cluster.channels[0];
    var two = cluster.channels[1];
    var hostOne = cluster.hosts[0];

    one.handler = EndpointHandler();

    one.handler.register('/error', function error(req, res) {
        res.sendNotOk(null, 'abc');
    });

    one.handler.register('/error-frame', function errorFrame(req, res) {
        res.sendErrorFrame('Busy', 'some message');
    });

    one.handler.register('/buffer-head', function buffer(req, res) {
        res.sendOk(new Buffer('abc'), null);
    });
    one.handler.register('/string-head', function string(req, res) {
        res.sendOk('abc', null);
    });
    one.handler.register('/object-head', function object(req, res) {
        res.sendOk(JSON.stringify({ value: 'abc' }), null);
    });
    one.handler.register('/null-head', function nullH(req, res) {
        res.sendOk(null, null);
    });
    one.handler.register('/undef-head', function undefH(req, res) {
        res.sendOk(undefined, null);
    });

    one.handler.register('/buffer-body', function buffer(req, res) {
        res.sendOk(null, new Buffer('abc'));
    });
    one.handler.register('/string-body', function string(req, res) {
        res.sendOk(null, 'abc');
    });
    one.handler.register('/object-body', function object(req, res) {
        res.sendOk(null, JSON.stringify({ value: 'abc' }));
    });
    one.handler.register('/null-body', function nullB(req, res) {
        res.sendOk(null, null);
    });
    one.handler.register('/undef-body', function undefB(req, res) {
        res.sendOk(null, undefined);
    });

    parallel({
        'errorCall': sendCall.bind(null, two, {
            host: hostOne
        }, '/error'),
        'errorFrameCall': sendCall.bind(null, two, {
            host: hostOne
        }, '/error-frame'),

        'bufferHead': sendCall.bind(null, two, {
            host: hostOne
        }, '/buffer-head'),
        'stringHead': sendCall.bind(null, two, {
            host: hostOne
        }, '/string-head'),
        'objectHead': sendCall.bind(null, two, {
            host: hostOne
        }, '/object-head'),
        'nullHead': sendCall.bind(null, two, {
            host: hostOne
        }, '/null-head'),
        'undefHead': sendCall.bind(null, two, {
            host: hostOne
        }, '/undef-head'),

        'bufferBody': sendCall.bind(null, two, {
            host: hostOne
        }, '/buffer-body'),
        'stringBody': sendCall.bind(null, two, {
            host: hostOne
        }, '/string-body'),
        'objectBody': sendCall.bind(null, two, {
            host: hostOne
        }, '/object-body'),
        'nullBody': sendCall.bind(null, two, {
            host: hostOne
        }, '/null-body'),
        'undefBody': sendCall.bind(null, two, {
            host: hostOne
        }, '/undef-body')
    }, onResults);

    function onResults(err, results) {
        assert.ifError(err);

        var errorCall = results.errorCall;
        assert.equal(errorCall.err, null);
        assert.ok(Buffer.isBuffer(errorCall.head));
        assert.equal(String(errorCall.head), '');
        assert.ok(Buffer.isBuffer(errorCall.body));
        assert.equal(String(errorCall.body), 'abc');

        var errorFrameCall = results.errorFrameCall;
        var frameErr = errorFrameCall.err;
        assert.equal(frameErr.type, 'tchannel.busy');
        assert.equal(frameErr.isErrorFrame, true);
        assert.equal(frameErr.errorCode, 3);
        assert.equal(typeof frameErr.originalId, 'number');
        assert.equal(frameErr.message, 'some message');
        assert.equal(errorFrameCall.head, null);
        assert.equal(errorFrameCall.body, null);

        var bufferHead = results.bufferHead;
        assert.equal(bufferHead.err, null);
        assert.ok(Buffer.isBuffer(bufferHead.head));
        assert.equal(String(bufferHead.head), 'abc');
        assert.ok(Buffer.isBuffer(bufferHead.body));
        assert.equal(String(bufferHead.body), '');

        var stringHead = results.stringHead;
        assert.equal(stringHead.err, null);
        assert.ok(Buffer.isBuffer(stringHead.head));
        assert.equal(String(stringHead.head), 'abc');
        assert.ok(Buffer.isBuffer(stringHead.body));
        assert.equal(String(stringHead.body), '');

        var objectHead = results.objectHead;
        assert.equal(objectHead.err, null);
        assert.ok(Buffer.isBuffer(objectHead.head));
        assert.equal(String(objectHead.head), '{"value":"abc"}');
        assert.ok(Buffer.isBuffer(objectHead.body));
        assert.equal(String(objectHead.body), '');

        var nullHead = results.nullHead;
        assert.equal(nullHead.err, null);
        assert.ok(Buffer.isBuffer(nullHead.head));
        assert.equal(String(nullHead.head), '');
        assert.ok(Buffer.isBuffer(nullHead.body));
        assert.equal(String(nullHead.body), '');

        var undefHead = results.undefHead;
        assert.equal(undefHead.err, null);
        assert.ok(Buffer.isBuffer(undefHead.head));
        assert.equal(String(undefHead.head), '');
        assert.ok(Buffer.isBuffer(undefHead.body));
        assert.equal(String(undefHead.body), '');

        var bufferBody = results.bufferBody;
        assert.equal(bufferBody.err, null);
        assert.ok(Buffer.isBuffer(bufferBody.head));
        assert.equal(String(bufferBody.head), '');
        assert.ok(Buffer.isBuffer(bufferBody.body));
        assert.equal(String(bufferBody.body), 'abc');

        var stringBody = results.stringBody;
        assert.equal(stringBody.err, null);
        assert.ok(Buffer.isBuffer(stringBody.head));
        assert.equal(String(stringBody.head), '');
        assert.ok(Buffer.isBuffer(stringBody.body));
        assert.equal(String(stringBody.body), 'abc');

        var objectBody = results.objectBody;
        assert.equal(objectBody.err, null);
        assert.ok(Buffer.isBuffer(objectBody.head));
        assert.equal(String(objectBody.head), '');
        assert.ok(Buffer.isBuffer(objectBody.body));
        assert.equal(String(objectBody.body), '{"value":"abc"}');

        var nullBody = results.nullBody;
        assert.equal(nullBody.err, null);
        assert.ok(Buffer.isBuffer(nullBody.head));
        assert.equal(String(nullBody.head), '');
        assert.ok(Buffer.isBuffer(nullBody.body));
        assert.equal(String(nullBody.body), '');

        var undefBody = results.undefBody;
        assert.equal(undefBody.err, null);
        assert.ok(Buffer.isBuffer(undefBody.head));
        assert.equal(String(undefBody.head), '');
        assert.ok(Buffer.isBuffer(undefBody.body));
        assert.equal(String(undefBody.body), '');

        assert.end();
    }
});

function sendCall(channel, opts, op, cb) {
    channel
        .request(opts)
        .send(op, null, null, onResult);

    function onResult(err, res) {
        cb(null, {
            err: err,
            head: res && res.arg2,
            body: res && res.arg3
        });
    }
}
