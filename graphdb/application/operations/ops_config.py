from __future__ import annotations

from typing import Any, Dict
from graphdb.application.policies.pol_config_redaction import ConfigRedactionPolicy
from graphdb.domain.config import GraphDBConfig


class ConfigOperations:
    """Use case orchestrator for loading, presenting, and redacting configuration data."""

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self.redaction_policy = ConfigRedactionPolicy()

    def get_redacted_config(self) -> Dict[str, Any]:
        """Returns the configuration payload with sensitive fields masked."""
        raw_dict = self.config.to_dict() if hasattr(self.config, "to_dict") else vars(self.config)
        return self.redaction_policy.redact(raw_dict)

    def redact_data(self, data: Any) -> Any:
        """Applies the redaction policy to an arbitrary dictionary or list structure."""
        return self.redaction_policy.redact(data)
