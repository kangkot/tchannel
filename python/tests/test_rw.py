# encoding=utf8
from __future__ import absolute_import

from collections import namedtuple

import pytest
from doubles import allow, expect, InstanceDouble

from tchannel import rw
from tchannel.io import BytesIO


def bio(bs):
    return BytesIO(bytearray(bs))


def test_none_r():
    stream = BytesIO('a b c')
    assert rw.none().read(stream) is None
    stream.read() == 'a b c'

    assert rw.none().width() == 0


def test_none_w():
    stream = BytesIO()
    assert rw.none().write(42, stream) == stream
    assert stream.getvalue() == ''


@pytest.mark.parametrize('other, bs', [
    (rw.none(), []),
    (rw.number(1), [1]),
    (rw.number(4), [1, 2, 3, 4]),
])
def test_constant_r(other, bs):
    stream = bio(bs)
    assert rw.constant(other, 42).read(stream) == 42
    assert stream.read() == ''

    assert rw.constant(other, 42).width() == other.width()


@pytest.mark.parametrize('other, v1, v2', [
    (rw.none(), None, None),
    (rw.number(1), 10, 12),
    (rw.number(2), 500, 1),
    (rw.len_prefixed_string(rw.number(1)), "hello world", "test"),
])
def test_constant_w(other, v1, v2):
    # constant(f, x).write(s, *) == f.write(x, *)
    assert other.write(
        v1, BytesIO()
    ).getvalue() == rw.constant(other, v1).write(
        v2, BytesIO()
    ).getvalue()


@pytest.mark.parametrize('num, width, bs', [
    (42, 1, [42]),
    (258, 2, [1, 2]),
    (16909060, 4, [1, 2, 3, 4]),
    (283686952306183, 8, [0, 1, 2, 3, 4, 5, 6, 7]),
])
def test_number(num, width, bs):
    assert rw.number(width).read(bio(bs)) == num
    assert rw.number(width).write(num, BytesIO()).getvalue() == bytearray(bs)
    assert rw.number(width).width() == width


@pytest.mark.parametrize('s, len_width, bs', [
    ('', 1, [0]),
    (u"☃", 2, [0, 3, 0xe2, 0x98, 0x83]),
    ('hello world', 4, [0, 0, 0, 11] + list('hello world')),
])
def test_len_prefixed_string(s, len_width, bs):
    s_rw = rw.len_prefixed_string(rw.number(len_width))
    assert s_rw.read(bio(bs)) == s
    assert s_rw.write(s, BytesIO()).getvalue() == bytearray(bs)

    assert s_rw.width() == len_width


@pytest.mark.parametrize('s, len_width, bs', [
    (b"\xe2\x98\x83", 2, [0, 3, 0xe2, 0x98, 0x83]),
    ('hello world', 4, [0, 0, 0, 11] + list('hello world'))
])
def test_len_prefixed_string_binary(s, len_width, bs):
    s_rw = rw.len_prefixed_string(rw.number(len_width), is_binary=True)
    assert s_rw.read(bio(bs)) == s
    assert s_rw.write(s, BytesIO()).getvalue() == bytearray(bs)

    assert s_rw.width() == len_width


def test_chain_with_list():
    assert rw.chain(
        [rw.number(1), rw.number(2)]
    ).read(bio([1, 2, 3])) == [1, 515]


@pytest.mark.parametrize('values, width, links, bs', [
    ((), 0, [], []),
    ((1, 2, 3), 7, [rw.number(1), rw.number(2), rw.number(4)], [
        1, 0, 2, 0, 0, 0, 3
    ]),
])
def test_chain(values, width, links, bs):
    assert list(rw.chain(*links).read(bio(bs))) == list(values)
    assert rw.chain(*links).write(
        values, BytesIO()
    ).getvalue() == bytearray(bs)

    assert rw.chain(*links).width() == width


@pytest.mark.parametrize('value, width, pairs, bs', [
    ({}, 0, [], []),
    ({'x': 1, 'y': 2}, 3, [('x', rw.number(1)), ('y', rw.number(2))], [
        1, 0, 2
    ]),
])
def test_dictionary(value, width, pairs, bs):
    assert rw.dictionary(*pairs).read(bio(bs)) == value
    assert rw.dictionary(*pairs).write(
        value, BytesIO()
    ).getvalue() == bytearray(bs)

    assert rw.dictionary(*pairs).width() == width


def test_dictionary_read_error():
    some_rw = InstanceDouble('tchannel.rw.ReadWriter')
    allow(some_rw).read.and_raise(rw.ReadException('great sadness'))

    dict_rw = rw.dictionary(('foo', some_rw))
    with pytest.raises(rw.ReadException):
        dict_rw.read(BytesIO())


def test_dictionary_ignore_fields():
    d_rw = rw.dictionary(
        ('x', rw.number(1)),
        (rw.skip, rw.constant(rw.number(2), 42)),
    )

    assert d_rw.read(bio([1, 0, 2])) == {'x': 1}
    assert d_rw.write(
        {'x': 1, rw.skip: 2, 'y': 3}, BytesIO()
    ).getvalue() == bytearray([1, 0, 42])

    assert d_rw.width() == 3


NoArgsConstructor = namedtuple('NoArgsConstructor', [])
ClassWithArgs = namedtuple('ClassWithArgs', ['x', 'y'])


@pytest.mark.parametrize('obj, width, params, bs', [
    (NoArgsConstructor(), 0, [NoArgsConstructor], []),
    (ClassWithArgs(1, 2), 3, [
        ClassWithArgs, ('x', rw.number(1)), ('y', rw.number(2))
    ], [1, 0, 2]),
])
def test_instance(obj, width, params, bs):
    i_rw = rw.instance(*params)
    assert i_rw.read(bio(bs)) == obj
    assert i_rw.write(obj, BytesIO()).getvalue() == bytearray(bs)

    assert i_rw.width() == width


def test_instance_exception():
    some_rw = InstanceDouble('tchannel.rw.ReadWriter')
    c_rw = rw.instance(ClassWithArgs, ('x', some_rw), ('y', rw.number(4)))
    allow(some_rw).read.and_raise(rw.ReadException("great sadness"))

    with pytest.raises(rw.ReadException):
        c_rw.read(bio([1, 2, 3, 4]))


def test_instance_ignore():
    c_rw = rw.instance(
        ClassWithArgs,
        ('x', rw.number(1)),
        (rw.skip, rw.constant(rw.number(2), 42)),
        ('y', rw.number(1)),
    )

    assert c_rw.read(bio([1, 2, 3, 4])) == ClassWithArgs(1, 4)

    assert c_rw.write(
        ClassWithArgs(1, 2), BytesIO()
    ).getvalue() == bytearray([1, 0, 42, 2])


@pytest.mark.parametrize('l_rw, k_rw, v_rw, headers, bs', [
    (rw.number(1), rw.len_prefixed_string(rw.number(1)), None, [], [0]),
    (rw.number(1), rw.len_prefixed_string(rw.number(1)), None, [
        ['hello', 'world'],
        ['hello', 'world'],  # with dupe
    ], [2] + ([5] + list('hello') + [5] + list('world')) * 2),
])
def test_headers(l_rw, k_rw, v_rw, headers, bs):
    h_rw = rw.headers(l_rw, k_rw, v_rw)
    assert h_rw.read(bio(bs)) == headers
    assert h_rw.write(headers, BytesIO()).getvalue() == bytearray(bs)

    assert h_rw.width() == l_rw.width()


def test_headers_with_dict():
    h_rw = rw.headers(
        rw.number(2),
        rw.len_prefixed_string(rw.number(2)),
        rw.len_prefixed_string(rw.number(1))
    )

    headers = {
        'hello': 'world',
        'this': 'is a test',
    }

    buff = h_rw.write(headers, BytesIO()).getvalue()
    assert sorted(h_rw.read(bio(buff)), key=lambda x: x[0]) == [
        ['hello', 'world'],
        ['this', 'is a test']
    ]


_test_switch_cases = {0: rw.none(), 1: rw.number(1), 2: rw.number(2)}


@pytest.mark.parametrize('switch_rw, cases, width, value, bs', [
    (rw.number(1), _test_switch_cases, 1, (0, None), [0]),
    (rw.number(1), _test_switch_cases, 1, (1, 42), [1, 42]),
    (rw.number(1), _test_switch_cases, 1, (2, 42), [2, 0, 42]),
    (rw.number(1), _test_switch_cases, 1, (4, None), [4]),
    (rw.len_prefixed_string(rw.number(1)), {
        'a': rw.number(1),
        'b': rw.number(2)
    }, 1, ('b', 12), [1, ord('b'), 0, 12]),
])
def test_switch(switch_rw, cases, width, value, bs):
    s_rw = rw.switch(switch_rw, cases)
    assert s_rw.read(bio(bs)) == value
    assert s_rw.write(value, BytesIO()).getvalue() == bytearray(bs)

    assert s_rw.width() == width


@pytest.mark.parametrize('t_rw, bs', [
    (rw.number(1), []),
    (rw.number(2), [1]),
    (rw.number(4), [1, 2, 3]),
    (rw.number(8), range(7)),
    (rw.len_prefixed_string(rw.number(1)), [10, 97]),
    (rw.chain(rw.number(1), rw.number(2)), [1, 2]),
    (rw.switch(rw.number(1), {0: rw.number(2)}), [0, 1]),
])
def test_stream_too_short(t_rw, bs):
    with pytest.raises(rw.ReadException):
        t_rw.read(bio(bs))


class TestDelegatingReadWriter(object):

    def _mk_rw(self, t_rw):
        return type(
            'SomeClass',
            (rw.DelegatingReadWriter,),
            {'__rw__': t_rw}
        )()

    def test_child_class_must_have_rw(self):
        # Ensure that we require child classes of DelegatingReadWriter to have
        # __rw__
        with pytest.raises(AssertionError):
            type(
                'SomeClass',
                (rw.DelegatingReadWriter,),
                {'__rw': rw.number(1)},
            )

    def test_read(self):
        some_rw = InstanceDouble('tchannel.rw.ReadWriter')
        delegated_rw = self._mk_rw(some_rw)

        stream = BytesIO()

        expect(some_rw).read.with_args(stream)
        delegated_rw.read(stream)

    def test_write(self):
        some_rw = InstanceDouble('tchannel.rw.ReadWriter')
        delegated_rw = self._mk_rw(some_rw)

        stream = BytesIO()

        expect(some_rw).write.with_args("foo", stream)
        delegated_rw.write("foo", stream)

    def test_width(self):
        some_rw = InstanceDouble('tchannel.rw.ReadWriter')
        delegated_rw = self._mk_rw(some_rw)

        expect(some_rw).width.with_no_args()
        delegated_rw.width()
