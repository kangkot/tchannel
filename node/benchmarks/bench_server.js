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

var TChannel = require('../index');
var EndpointHandler = require('../endpoint-handler')
var server = new TChannel({
	handler: EndpointHandler()
});
server.listen(4040, '127.0.0.1');

var keys = {};

server.on('socketClose', function (conn, err) {
	// console.log('socket close: ' + conn.remoteName + ' ' + err);
});

server.handler.register('ping', function onPing(req, res) {
	res.sendOk('pong', null);
});

function safeParse(str) {
	try {
		return JSON.parse(str);
	} catch (e) {
		return null;
	}
}

server.handler.register('set', function onSet(req, res) {
	var parts = safeParse(req.arg2.toString('utf8'));
	keys[parts[0]] = parts[1];
	res.sendOk('ok', 'really ok');
});

server.handler.register('get', function onGet(req, res) {
	var str = req.arg2.toString('utf8');
	if (keys[str] !== undefined) {
		res.sendOk(JSON.stringify(keys[str].length), JSON.stringify(keys[str]));
	} else {
		res.sendNotOk(null, 'key not found: ' + str);
	}
});

// setInterval(function () {
// 	Object.keys(keys).forEach(function (key) {
// 		console.log(key + '=' + keys[key].length + ' bytes');
// 	});
// }, 1000);
