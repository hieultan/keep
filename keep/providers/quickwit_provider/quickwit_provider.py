"""Quickwit Provider for Keep.

This provider allows querying Quickwit using the Aggregations API.
"""

import dataclasses
from typing import Optional

import pydantic
import requests
from requests.auth import HTTPBasicAuth

from keep.contextmanager.contextmanager import ContextManager
from keep.providers.base.base_provider import BaseProvider, ProviderHealthMixin
from keep.providers.models.provider_config import ProviderConfig, ProviderScope


@pydantic.dataclasses.dataclass
class QuickwitProviderAuthConfig:
    """Authentication configuration for Quickwit."""

    host_url: pydantic.AnyHttpUrl = dataclasses.field(
        metadata={
            "required": True,
            "description": "Quickwit host URL",
            "hint": "http://localhost:7280",
            "validation": "any_http_url",
        }
    )
    verify: bool = dataclasses.field(
        default=True,
        metadata={
            "description": "Verify SSL certificates",
            "hint": "Set to false to allow self-signed certificates",
            "type": "switch",
        },
    )
    username: Optional[str] = dataclasses.field(
        default=None,
        metadata={"description": "Username for basic auth"},
    )
    password: Optional[str] = dataclasses.field(
        default=None,
        metadata={"description": "Password for basic auth", "sensitive": True},
    )
    token: Optional[str] = dataclasses.field(
        default=None,
        metadata={"description": "Bearer token", "sensitive": True},
    )


class QuickwitProvider(BaseProvider, ProviderHealthMixin):
    """Query data from Quickwit using its Aggregations API."""

    PROVIDER_DISPLAY_NAME = "Quickwit"
    PROVIDER_CATEGORY = ["Monitoring"]
    PROVIDER_SCOPES = [
        ProviderScope(
            name="connectivity", description="Connectivity Test", mandatory=True
        )
    ]

    def __init__(
        self, context_manager: ContextManager, provider_id: str, config: ProviderConfig
    ):
        super().__init__(context_manager, provider_id, config)

    def dispose(self):
        return

    def validate_config(self):
        self.authentication_config = QuickwitProviderAuthConfig(
            **self.config.authentication
        )

        """Validate that the Quickwit server is reachable."""

        url = f"{self.authentication_config.host_url}/api/v1/version".rstrip("/")
        try:
            response = requests.get(url, verify=self.authentication_config.verify)
            response.raise_for_status()
            return {"connectivity": True}
        except Exception as e:
            return {"connectivity": str(e)}

    def _build_auth(self):
        if self.authentication_config.username and self.authentication_config.password:
            return HTTPBasicAuth(
                self.authentication_config.username, self.authentication_config.password
            )
        return None

    def _build_headers(self):
        headers = {}
        if self.authentication_config.token:
            headers["Authorization"] = f"Bearer {self.authentication_config.token}"
        return headers

    def _query(
        self,
        index_id: str,
        query: str,
        aggs: Optional[dict] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        max_hits: Optional[int] = None,
        **_: dict,
    ) -> dict:
        """Run a query against Quickwit."""

        body: dict = {"query": query}
        if aggs is not None:
            body["aggs"] = aggs
        if start is not None:
            body["start"] = start
        if end is not None:
            body["end"] = end
        if max_hits is not None:
            body["max_hits"] = max_hits

        url = f"{self.authentication_config.host_url}/api/v1/{index_id}/search"
        response = requests.post(
            url,
            json=body,
            headers=self._build_headers(),
            auth=self._build_auth(),
            verify=self.authentication_config.verify,
        )
        response.raise_for_status()
        return response.json()
