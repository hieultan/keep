import dataclasses
import json
import logging
import time
from typing import Optional

import pydantic
from google.cloud import pubsub_v1

from keep.contextmanager.contextmanager import ContextManager
from keep.providers.base.base_provider import BaseProvider
from keep.providers.gcpmonitoring_provider.gcpmonitoring_provider import (
    GcpmonitoringProvider,
)
from keep.providers.models.provider_config import ProviderConfig, ProviderScope
from keep.providers.providers_factory import ProvidersFactory


@pydantic.dataclasses.dataclass
class GcppubsubProviderAuthConfig:
    service_account_json: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Service account JSON with permissions to read the subscription",
            "sensitive": True,
            "type": "file",
            "name": "service_account_json",
            "file_type": "application/json",
        },
    )
    subscription: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Pub/Sub subscription path (projects/<project>/subscriptions/<name>)",
        }
    )


class GcppubsubProvider(BaseProvider):
    """Consume messages from Google PubSub and push them as alerts to Keep."""

    PROVIDER_DISPLAY_NAME = "Google PubSub"
    PROVIDER_CATEGORY = ["Queues"]
    PROVIDER_TAGS = ["queue"]
    PROVIDER_SCOPES = [
        ProviderScope(
            name="pubsub.subscriber",
            description="Read access to the Pub/Sub subscription",
            mandatory=True,
            alias="Subscriber",
        )
    ]

    def __init__(
        self, context_manager: ContextManager, provider_id: str, config: ProviderConfig
    ):
        super().__init__(context_manager, provider_id, config)
        self._client: pubsub_v1.SubscriberClient | None = None
        self.consume = False

    def validate_config(self):
        if self.config.authentication is None:
            self.config.authentication = {}
        self.authentication_config = GcppubsubProviderAuthConfig(
            **self.config.authentication
        )

    def _get_client(self) -> pubsub_v1.SubscriberClient:
        if not self._client:
            if self.authentication_config.service_account_json:
                info = json.loads(self.authentication_config.service_account_json)
                self._client = pubsub_v1.SubscriberClient.from_service_account_info(
                    info
                )
            else:
                self._client = pubsub_v1.SubscriberClient()
        return self._client

    def dispose(self):
        if self._client:
            self._client.close()

    def start_consume(self):
        self.consume = True
        client = self._get_client()
        subscription = self.authentication_config.subscription
        self.logger.info(f"Starting PubSub consumption from {subscription}")
        while self.consume:
            response = client.pull(
                subscription=subscription, max_messages=10, timeout=5
            )
            if not response.received_messages:
                time.sleep(1)
                continue
            ack_ids = []
            for msg in response.received_messages:
                ack_ids.append(msg.ack_id)
                try:
                    data = msg.message.data.decode()
                    event = json.loads(data)
                    alert = GcpmonitoringProvider._format_alert(event, self)
                    self._push_alert(alert.dict())
                except Exception:
                    self.logger.exception("Failed to process PubSub message")
            if ack_ids:
                client.acknowledge(subscription=subscription, ack_ids=ack_ids)

    def stop_consume(self):
        self.consume = False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    from keep.api.core.dependencies import SINGLE_TENANT_UUID

    context_manager = ContextManager(tenant_id=SINGLE_TENANT_UUID)
    config = {
        "authentication": {
            "subscription": "projects/my-project/subscriptions/my-subscription",
            # "service_account_json": "{}",
        }
    }
    provider = ProvidersFactory.get_provider(
        context_manager,
        provider_id="gcppubsub-demo",
        provider_type="gcppubsub",
        provider_config=config,
    )
    provider.start_consume()
