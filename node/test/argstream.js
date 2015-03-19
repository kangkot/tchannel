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

var testExpectations = require('./lib/test_expectations');
var test = require('tape');
var argstream = require('../argstream');

/* jshint camelcase:false */

var Cases = [

    {
        name: '..',
        frames: [
            ['', '', '']
        ],
        run: function basicABC(s) {
            s.arg1.end();
            s.arg2.end();
            s.arg3.end();
        },
        arg1: null,
        arg2: null,
        arg3: null
    },

    {
        name: '._.',
        frames: [
            ['', ''], // arg1, arg2
            ['', '']  // arg2, arg3
        ],
        run: function basicABC(s) {
            s.arg1.end();
            setTimeout(function() {
                s.arg2.end();
                s.arg3.end();
            }, 2);
        },
        arg1: null,
        arg2: null,
        arg3: null
    },

    {
        name: '.._',
        frames: [
            ['', '', ''], // arg1, arg2, arg3
            ['']          // arg3
        ],
        run: function basicABC(s) {
            s.arg1.end();
            s.arg2.end();
            setTimeout(function() {
                s.arg3.end();
            }, 2);
        },
        arg1: null,
        arg2: null,
        arg3: null
    },

    {
        name: '._._',
        frames: [
            ['', ''], // arg1, arg2,
            ['', ''], // arg2, arg3
            ['']      // arg3
        ],
        run: function basicABC(s) {
            s.arg1.end();
            setTimeout(function() {
                s.arg2.end();
                setTimeout(function() {
                    s.arg3.end();
                }, 2);
            }, 2);
        },
        arg1: null,
        arg2: null,
        arg3: null
    },

    {
        name: '_._._',
        frames: [
            ['', ''], // arg1, arg2,
            ['', ''], // arg2, arg3
            ['']      // arg3
        ],
        run: function basicABC(s) {
            setTimeout(function() {
                s.arg1.end();
                setTimeout(function() {
                    s.arg2.end();
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: null,
        arg2: null,
        arg3: null
    },

    {
        name: 'a_._._',
        frames: [
            ['a'],    // arg1
            ['', ''], // arg1, arg2,
            ['', ''], // arg2, arg3
            ['']      // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end();
                setTimeout(function() {
                    s.arg2.end();
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'a',
        arg2: null,
        arg3: null
    },

    {
        name: 'a_.b_._',
        frames: [
            ['a'],     // arg1
            ['', 'b'], // arg1, arg2,
            ['', ''],  // arg2, arg3
            ['']       // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end();
                s.arg2.write('b');
                setTimeout(function() {
                    s.arg2.end();
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'a',
        arg2: 'b',
        arg3: null
    },

    {
        name: 'a_.b_.c_',
        frames: [
            ['a'],     // arg1
            ['', 'b'], // arg1, arg2,
            ['', 'c'], // arg2, arg3
            ['']       // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end();
                s.arg2.write('b');
                setTimeout(function() {
                    s.arg2.end();
                    s.arg3.write('c');
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'a',
        arg2: 'b',
        arg3: 'c'
    },

    {
        name: 'a_b.c_.d_',
        frames: [
            ['a'],      // arg1
            ['b', 'c'], // arg1, arg2,
            ['', 'd'],  // arg2, arg3
            ['']        // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end('b');
                s.arg2.write('c');
                setTimeout(function() {
                    s.arg2.end();
                    s.arg3.write('d');
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'ab',
        arg2: 'c',
        arg3: 'd'
    },

    {
        name: 'a_b.c_d.e_',
        frames: [
            ['a'],      // arg1
            ['b', 'c'], // arg1, arg2,
            ['d', 'e'], // arg2, arg3
            ['']        // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end('b');
                s.arg2.write('c');
                setTimeout(function() {
                    s.arg2.end('d');
                    s.arg3.write('e');
                    setTimeout(function() {
                        s.arg3.end();
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'ab',
        arg2: 'cd',
        arg3: 'e'
    },

    {
        name: 'a_b.c_d.e_f',
        frames: [
            ['a'],      // arg1
            ['b', 'c'], // arg1, arg2,
            ['d', 'e'], // arg2, arg3
            ['f']       // arg3
        ],
        run: function basicABC(s) {
            s.arg1.write('a');
            setTimeout(function() {
                s.arg1.end('b');
                s.arg2.write('c');
                setTimeout(function() {
                    s.arg2.end('d');
                    s.arg3.write('e');
                    setTimeout(function() {
                        s.arg3.end('f');
                    }, 2);
                }, 2);
            }, 2);
        },
        arg1: 'ab',
        arg2: 'cd',
        arg3: 'ef'
    },

    {
        name: 'a..',
        frames: [
            ['a', '', '']
        ],
        run: function basicABC(s) {
            s.arg1.end('a');
            s.arg2.end();
            s.arg3.end();
        },
        arg1: 'a',
        arg2: null,
        arg3: null
    },

    {
        name: '.b.',
        frames: [
            ['', 'b', '']
        ],
        run: function basicABC(s) {
            s.arg1.end();
            s.arg2.end('b');
            s.arg3.end();
        },
        arg1: null,
        arg2: 'b',
        arg3: null
    },

    {
        name: '..c',
        frames: [
            ['', '', 'c']
        ],
        run: function basicABC(s) {
            s.arg1.end();
            s.arg2.end();
            s.arg3.end('c');
        },
        arg1: null,
        arg2: null,
        arg3: 'c'
    },

    {
        name: 'a..c',
        frames: [
            ['a', '', 'c']
        ],
        run: function basicABC(s) {
            s.arg1.end('a');
            s.arg2.end();
            s.arg3.end('c');
        },
        arg1: 'a',
        arg2: null,
        arg3: 'c'
    },

    {
        name: '.b.c',
        frames: [
            ['', 'b', 'c']
        ],
        run: function basicABC(s) {
            s.arg1.end();
            s.arg2.end('b');
            s.arg3.end('c');
        },
        arg1: null,
        arg2: 'b',
        arg3: 'c'
    },

    {
        name: 'a.b.',
        frames: [
            ['a', 'b', '']
        ],
        run: function basicABC(s) {
            s.arg1.end('a');
            s.arg2.end('b');
            s.arg3.end();
        },
        arg1: 'a',
        arg2: 'b',
        arg3: null
    },

    {
        name: 'a.b.c',
        frames: [
            ['a', 'b', 'c']
        ],
        run: function basicABC(s) {
            s.arg1.end('a');
            s.arg2.end('b');
            s.arg3.end('c');
        },
        arg1: 'a',
        arg2: 'b',
        arg3: 'c'
    },

    {
        name: 'a.bc.de',
        frames: [
            ['a', 'bc', 'de']
        ],
        run: function fiveParts(s) {
            s.arg1.end('a');
            s.arg2.write('b');
            s.arg2.end('c');
            s.arg3.write('d');
            s.arg3.end('e');
        },
        arg1: 'a',
        arg2: 'bc',
        arg3: 'de'
    },

    {
        name: 'a.b._c',
        frames: [
            ['a', 'b', ''],
            ['c']
        ],
        run: function delayedAB_C(s) {
            s.arg1.end('a');
            s.arg2.end('b');
            setTimeout(function() {
                s.arg3.end('c');
            }, 2);
        },
        arg1: 'a',
        arg2: 'b',
        arg3: 'c'
    },

    {
        name: 'a.b_c.d_e',
        frames: [
            ['a', 'b'],
            ['c', 'd'],
            ['e']
        ],
        run: function delayedAB_CD_E(s) {
            s.arg1.end('a');
            s.arg2.write('b');
            setTimeout(function() {
                s.arg2.end('c');
                s.arg3.write('d');
                setTimeout(function() {
                    s.arg3.end('e');
                }, 2);
            }, 2);
        },
        arg1: 'a',
        arg2: 'bc',
        arg3: 'de'
    },

    {
        name: 'a.bc._d_e',
        frames: [
            ['a', 'bc', ''],
            ['d'],
            ['e']
        ],
        run: function delayedAB_CD_E(s) {
            s.arg1.end('a');
            s.arg2.write('b');
            s.arg2.end('c');
            setTimeout(function() {
                s.arg3.write('d');
                setTimeout(function() {
                    s.arg3.end('e');
                }, 2);
            }, 2);
        },
        arg1: 'a',
        arg2: 'bc',
        arg3: 'de'
    },

];

// unit test OutArgStream: XXX
Cases.forEach(function eachTestCase(testCase) {
    testExpectations('OutArgStream: ' + testCase.name,
        testCase.frames.map(function(parts, i) {
            var desc = 'expected frame parts[' + i + ']';
            return {frame: function(passed, assert) {
                assert.deepEqual(passed, parts, desc);
            }};
        }), function t(expect, finish) {
            var s = new argstream.OutArgStream();
            s.on('frame', function onFrame(parts) {
                parts = parts.map(function(b) {return b.toString();});
                expect('frame', parts);
            });
            hookupEnd(s, finish);
            testCase.run(s);
        });
});

// unit test InArgStream: XXX
Cases.forEach(function eachTestCase(testCase) {
    test('InArgStream: ' + testCase.name, function t(assert) {
        var s = new argstream.InArgStream();
        hookupEnd(s, function onFinish(err) {
            assert.ifError(err, 'no end error');
            assert.equal(getArg(s.arg1), testCase.arg1, 'expected arg1');
            assert.equal(getArg(s.arg2), testCase.arg2, 'expected arg2');
            assert.equal(getArg(s.arg3), testCase.arg3, 'expected arg3');
            assert.end();
        });
        testCase.frames.forEach(function eachFrame(parts) {
            s.handleFrame(parts);
        });
        s.handleFrame(null);
    });
});

// integration test {Out -> In}ArgStream: XXX
Cases.forEach(function eachTestCase(testCase) {
    test('{Out -> In}ArgStream: ' + testCase.name, function t(assert) {
        var o = new argstream.OutArgStream();
        var i = new argstream.InArgStream();
        o.on('frame', function onFrame(parts) {
            i.handleFrame(parts);
        });
        hookupEnd(o, function onFinish(err) {
            assert.ifError(err, 'no end error');
            i.handleFrame(null);
            assert.equal(getArg(i.arg1), testCase.arg1, 'expected arg1');
            assert.equal(getArg(i.arg2), testCase.arg2, 'expected arg2');
            assert.equal(getArg(i.arg3), testCase.arg3, 'expected arg3');
            assert.end();
        });
        testCase.run(o);
    });
});

function hookupEnd(stream, callback) {
    stream.on('finish', onFinish);
    stream.on('error', onError);
    function onError(err) {
        stream.removeListener('finish', onFinish);
        stream.removeListener('error', onError);
        callback(err);
    }
    function onFinish() {
        stream.removeListener('finish', onFinish);
        stream.removeListener('error', onError);
        callback();
    }
}

function getArg(arg) {
    var val = arg.read();
    if (val !== null) {
        return val.toString();
    } else {
        return val;
    }
}
