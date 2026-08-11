from __future__ import annotations

from typing import Any


class ConfigRedactionPolicy:
    """In-memory policy for redacting sensitive configuration values (e.g., passwords)."""

    @staticmethod
    def redact(data: Any) -> Any:
        if isinstance(data, dict):
            return {
                key: ("***REDACTED***" if "password" in key.lower() else ConfigRedactionPolicy.redact(value))
                for key, value in data.items()
            }
        if isinstance(data, list):
            return [ConfigRedactionPolicy.redact(item) for item in data]
        return data
