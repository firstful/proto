"""Tests for the PROTO plugin username-claim flow (requires cryptography pkg)."""
import base64
import json
import os
import sys
import threading
import unittest
from http.server import HTTPServer, BaseHTTPRequestHandler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from proto import plugin as P  # noqa: E402

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    HAVE_CRYPTO = True
except ImportError:
    HAVE_CRYPTO = False


class _FakeBroker(BaseHTTPRequestHandler):
    """Minimal broker: in-memory FCFS claim store, no signature check (the
    signature check itself is covered by the Go tests)."""

    claims = {}
    lock = threading.Lock()

    def log_message(self, *a):  # silence
        pass

    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/username_claims":
            with _FakeBroker.lock:
                self._send(list(_FakeBroker.claims.values()))
        else:
            self._send({})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/api/username_claims":
            with _FakeBroker.lock:
                nick = data.get("nickname", "")
                if nick in _FakeBroker.claims:
                    self._send({"ok": False, "winner": _FakeBroker.claims[nick]})
                else:
                    _FakeBroker.claims[nick] = data
                    self._send({"ok": True, "winner": data})
        else:
            self._send({})


@unittest.skipUnless(HAVE_CRYPTO, "cryptography not installed")
class TestClaimFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = HTTPServer(("127.0.0.1", 0), _FakeBroker)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        P._BROKER = f"http://127.0.0.1:{cls.port}"
        os.environ["PROTO_HANDLE"] = "hermes:test"
        P._CLAIMED_USERNAMES = {}

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def _make_key(self, tmpdir):
        from cryptography.hazmat.primitives import serialization
        priv = Ed25519PrivateKey.generate()
        pem = priv.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
        path = os.path.join(tmpdir, "test.key")
        with open(path, "wb") as f:
            f.write(pem)
        return path, priv

    def test_claim_success_and_cache(self):
        with self.temp_key() as (path, priv):
            P._KEY_PATHS.clear()
            self.assertTrue(P._claim_nickname("nick_a"))
            self.assertIn("nick_a", P._CLAIMED_USERNAMES)
            # Second attempt on a taken name by a different handle loses.
            os.environ["PROTO_HANDLE"] = "hermes:other"
            with self.temp_key() as (path2, _):
                P._KEY_PATHS.clear()
                self.assertFalse(P._claim_nickname("nick_a"))
            os.environ["PROTO_HANDLE"] = "hermes:test"
            P._KEY_PATH = path

    def test_restart_reclaim_same_handle_succeeds(self):
        with self.temp_key() as (path, priv):
            P._KEY_PATHS.clear()
            self.assertTrue(P._claim_nickname("nick_b"))
            P._CLAIMED_USERNAMES = {}  # simulate restart amnesia
            # Same handle re-claims: FCFS loss but winner is ours+valid -> True
            self.assertTrue(P._claim_nickname("nick_b"))

    def test_list_usernames(self):
        with self.temp_key() as (path, priv):
            P._KEY_PATHS.clear()
            P._claim_nickname("nick_c")
            out = P._handle_list_usernames({})
            self.assertGreaterEqual(out["total"], 1)
            nicks = [c["nickname"] for c in out["claimed_usernames"]]
            self.assertIn("nick_c", nicks)

    # -- helpers --
    import contextlib

    @contextlib.contextmanager
    def temp_key(self):
        import tempfile
        from cryptography.hazmat.primitives import serialization
        with tempfile.TemporaryDirectory() as d:
            priv = Ed25519PrivateKey.generate()
            pem = priv.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption())
            path = os.path.join(d, "k.key")
            with open(path, "wb") as f:
                f.write(pem)
            from pathlib import Path
            yield Path(path), priv


if __name__ == "__main__":
    unittest.main()
