# tests/test_util.py
from amp.util import to_hex


def test_to_hex():
    assert to_hex(b'\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef') == '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'
