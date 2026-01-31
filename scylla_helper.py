"""ScyllaDB connection helper with auto-detect IP for Colima/OrbStack."""

import os
import subprocess


def get_scylla_host():
    """
    Get ScyllaDB host IP address.
    Auto-detects container IP for Colima/OrbStack, falls back to env var or localhost.
    """
    # 1. Check environment variable first
    env_host = os.getenv("SCYLLA_HOST")
    if env_host:
        return env_host

    # 2. Auto-detect container IP using docker inspect
    try:
        result = subprocess.run(
            ["docker", "inspect", "riset-scylla",
             "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"],
            capture_output=True,
            text=True,
            timeout=10
        )
        print(result)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # 3. Fallback to localhost
    return "127.0.0.1"
