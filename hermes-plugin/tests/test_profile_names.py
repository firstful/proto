import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from proto import plugin as P  # noqa: E402


def _clear():
    for k in ("PROTO_HANDLE", "PROTO_NAME", "PROTO_NICKNAME",
              "HERMES_SESSION_PROFILE", "HERMES_PROFILE", "PROTO_DEFAULT_NAME"):
        os.environ.pop(k, None)


class TestProfileNames(unittest.TestCase):
    def tearDown(self):
        _clear()

    def test_profile_derivation(self):
        cases = [
            # (env, expected profile name)
            ({}, "rook"),  # default profile -> rook override
            ({"HERMES_SESSION_PROFILE": "comms"}, "comms"),
            ({"HERMES_PROFILE": "gaia"}, "gaia"),
            ({"HERMES_PROFILE": "kube", "PROTO_NAME": "kube-alt"}, "kube-alt"),
            ({"PROTO_HANDLE": "hermes:custom"}, "custom"),
            ({"HERMES_PROFILE": "default", "PROTO_DEFAULT_NAME": "reed"}, "reed"),
            # legacy nickname affects the claimed nickname, not identity
            ({"HERMES_SESSION_PROFILE": "sentinel", "PROTO_NICKNAME": "sen"},
             "sentinel"),
        ]
        for env, expected in cases:
            _clear()
            os.environ.update(env)
            got = P._profile_name()
            self.assertEqual(got, expected, f"env={env}")
            if "PROTO_HANDLE" not in env:
                self.assertEqual(P._handle(), f"hermes:{expected}", f"env={env}")

    def test_key_files_are_per_profile(self):
        # two profiles must map to distinct key files
        _clear()
        os.environ["HERMES_PROFILE"] = "gaia"
        p1, _ = P._get_keypair()
        _clear()
        os.environ["HERMES_PROFILE"] = "scribe"
        p2, _ = P._get_keypair()
        self.assertNotEqual(p1, p2)
        self.assertIn("gaia", str(p1))
        self.assertIn("scribe", str(p2))


if __name__ == "__main__":
    unittest.main()
