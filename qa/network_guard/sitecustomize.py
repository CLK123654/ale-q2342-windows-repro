from __future__ import annotations

import ipaddress
import os
import socket


_original_connect = socket.socket.connect


def _is_local(address) -> bool:
    if isinstance(address, str):
        return True
    if not isinstance(address, tuple) or not address:
        return True
    host = address[0]
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def guarded_connect(self, address):
    if not _is_local(address):
        raise OSError(f"external network disabled during formal execution: {address}")
    return _original_connect(self, address)


if os.environ.get("ALE_NETWORK_GUARD") != "1":
    raise RuntimeError("ALE_NETWORK_GUARD must be enabled")
socket.socket.connect = guarded_connect
