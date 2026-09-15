"""PROTO Hermes plugin — thin Python shim over the Go proto broker.

All protocol logic lives in the Go broker (gitea.ffilt.us/reeds/proto); this
package is a pure client. The full implementation (tools, username claims,
auto-connect) lives in ``plugin.py``; this __init__ is just the entrypoint
Hermes loads first.

    proto --addr :8808 --journal /mnt/hot/proto/journal.jsonl        # broker
    hermes-remote dispatch <peer> "<message>"                        # client
"""

from .plugin import *  # noqa: F401,F403 -- single source of truth
