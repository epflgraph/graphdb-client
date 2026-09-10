from __future__ import annotations

from typing import List, Optional, Protocol, runtime_checkable

from graphdb.application.ports.gateways.prt_environment import EnvironmentPort


@runtime_checkable
class EnvironmentRegistryPort(Protocol):
    """Port for a registry that lazily resolves environment ports by name.

    Application operations depend on this port rather than on the concrete
    ``Environments`` adapter, so the dependency direction points inward
    (application -> port) and fakes can be injected in tests.
    """

    def get(self, env_name: Optional[str] = None) -> EnvironmentPort: ...

    def names(self) -> List[str]: ...
