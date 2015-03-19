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

var async = require('async');
var Buffer = require('buffer').Buffer;
var extend = require('xtend');
var parallel = require('run-parallel');
var Ready = require('ready-signal');
var allocCluster = require('./lib/alloc-cluster.js');
var EndpointHandler = require('../endpoint-handler');

var Cases = [

    {
        name: 'stream body ["hello", " world"]',
        op: 'foo',
        reqHead: null,
        reqBody: [
            'hello',
            ' world'
        ],
        resHead: '',
        resBody: 'hello world'
    },

    {
        name: 'stream sec head + fox body',
        op: 'foo',
        reqHead: spaceWords('sic transit gloria mundi'),
        reqBody: spaceWords('the quick brown fox jumps over the lazy hound'),
        resHead: 'sic transit gloria mundi',
        resBody: 'the quick brown fox jumps over the lazy hound'
    },

    {
        name: 'stream abc head + 123 body',
        op: 'foo',
        reqHead: 'abcdef'.split(''),
        reqBody: '123456'.split(''),
        resHead: 'abcdef',
        resBody: '123456'
    },

];

allocCluster.test('streaming echo w/ buffered callback', 2, function t(cluster, assert) {
    var one = cluster.channels[0];
    var two = cluster.channels[1];
    var hostOne = cluster.hosts[0];

    one.handler = EndpointHandler();

    function foo(req, res) {
        res.setOK(true);

        req.arg2.on('data', function onArg2Data(chunk) {
            res.arg2.write(chunk);
        });
        req.arg2.on('end', function onArg2End() {
            res.arg2.end();
        });

        req.arg3.on('data', function onArg3Data(chunk) {
            res.arg3.write(chunk);
        });
        req.arg3.on('end', function onArg3End() {
            res.arg3.end();
        });

    }
    foo.canStream = true;
    one.handler.register('foo', foo);

    parallel(Cases.map(function eachTestCase(testCase) {
        testCase = extend({
            channel: two,
            opts: {host: hostOne},
        }, testCase);
        return sendTest(testCase, assert);
    }), function onResults(err) {
        assert.ifError(err, 'no errors from sending');

        var peersOne = one.getPeers();
        var peersTwo = two.getPeers();

        assert.equal(peersOne.length, 1, 'one should have 1 peer');
        assert.equal(peersTwo.length, 1, 'two should have 1 peer');

        var inPeer = peersOne[0];
        if (inPeer) {
            assert.equal(inPeer.direction, 'in', 'inPeer should be in');
            assert.equal(Object.keys(inPeer.inOps).length, 0, 'inPeer should have no inOps');
            assert.equal(Object.keys(inPeer.outOps).length, 0, 'inPeer should have no outOps');
        }

        var outPeer = peersTwo[0];
        if (outPeer) {
            assert.equal(outPeer.direction, 'out', 'outPeer should be out');
            assert.equal(Object.keys(outPeer.inOps).length, 0, 'outPeer should have no inOps');
            assert.equal(Object.keys(outPeer.outOps).length, 0, 'outPeer should have no outOps');
        }

        assert.end();
    });
});

function sendTest(testCase, assert) {
    return function runSendTest(callback) {
        var req = testCase.channel.request(testCase.opts);
        var resultReady = Ready();
        req.hookupCallback(resultReady.signal);
        req.arg1.end(testCase.op);

        async.series({
            sinkHead: sinkParts.bind(null, testCase.reqHead, req.arg2),
            sinkBody: sinkParts.bind(null, testCase.reqBody, req.arg3),
            result: resultReady
        }, onResult);

        function sinkParts(parts, stream, callback) {
            if (!parts) {
                stream.end();
                callback();
            } else {
                var i = 0;
                async.eachSeries(parts, function eachPart(part, next) {
                    console.log('write part %j', part);
                    if (++i < parts.length) {
                        stream.write(part);
                    } else {
                        stream.end(part);
                    }
                    setImmediate(next);
                }, callback);
            }
        }

        function onResult(err, result) {
            // var res = result.result[0];
            var head = result.result[1];
            var body = result.result[2];
            assert.ifError(err, testCase.name + ': no result error');
            if (!err) {
                assert.ok(Buffer.isBuffer(head), testCase.name + ': got head buffer');
                assert.ok(Buffer.isBuffer(body), testCase.name + ': got body buffer');
                assert.equal(head ? String(head) : head, testCase.resHead, testCase.name + ': expected head content');
                assert.equal(body ? String(body) : body, testCase.resBody, testCase.name + ': expected body content');
            }
            callback();
        }
    };
}

function spaceWords(str) {
    return str
        .split(/( +[^ ]+)/)
        .filter(function(part) {return part.length;});
}
