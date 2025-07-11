import dataclasses
import json
import logging
import time
from typing import Optional

import pydantic
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from keep.api.models.alert import AlertDto, AlertSeverity, AlertStatus
from keep.contextmanager.contextmanager import ContextManager
from keep.providers.base.base_provider import BaseProvider
from keep.providers.models.provider_config import ProviderConfig, ProviderScope
from keep.providers.providers_factory import ProvidersFactory
from keep.providers.gcpmonitoring_provider.gcpmonitoring_provider import (
    GcpmonitoringProvider,
)


@pydantic.dataclasses.dataclass
class GcppubsubProviderAuthConfig:
    project_id: str = dataclasses.field(
        metadata={"required": True, "description": "GCP project ID"}
    )
    subscription: str = dataclasses.field(
        metadata={"required": True, "description": "PubSub subscription name"}
    )
    service_account_json: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Service account JSON with pubsub subscriber role",
            "sensitive": True,
            "type": "file",
            "name": "service_account_json",
            "file_type": "application/json",
        },
    )


class GcppubsubProvider(BaseProvider):
    PROVIDER_DISPLAY_NAME = "GCP PubSub"
    PROVIDER_CATEGORY = ["Queues"]
    PROVIDER_TAGS = ["queue"]

    PROVIDER_SCOPES = [
        ProviderScope(
            name="roles/pubsub.subscriber",
            description="Read access to PubSub subscription",
            mandatory=True,
            alias="PubSub Subscriber",
        )
    ]

    def __init__(
        self, context_manager: ContextManager, provider_id: str, config: ProviderConfig
    ):
        super().__init__(context_manager, provider_id, config)
        self.consume = False
        self._client = None
        self._subscription_path = None

    def dispose(self):
        if self._client:
            self._client.close()

    def validate_config(self):
        if self.config.authentication is None:
            self.config.authentication = {}
        self.authentication_config = GcppubsubProviderAuthConfig(
            **self.config.authentication
        )
        self._subscription_path = pubsub_v1.SubscriberClient.subscription_path(
            self.authentication_config.project_id, self.authentication_config.subscription
        )

    # def _generate_client(self):
    #     if self.authentication_config.service_account_json:
    #         info = json.loads(self.authentication_config.service_account_json)
    #         credentials = service_account.Credentials.from_service_account_info(info)
    #         return pubsub_v1.SubscriberClient(credentials=credentials)
    #     return pubsub_v1.SubscriberClient()

    def _generate_client(self) -> pubsub_v1.SubscriberClient:
        if not self._client:
            if self.authentication_config.service_account_json:
                info = json.loads(self.authentication_config.service_account_json)
                self._client = pubsub_v1.SubscriberClient.from_service_account_info(
                    info
                )
            else:
                self._client = pubsub_v1.SubscriberClient()
        return self._client

    @property
    def client(self):
        if self._client is None:
            self._client = self._generate_client()
        return self._client

    def _message_to_alert(self, message) -> AlertDto:
        data_str = message.data.decode("utf-8") if message.data else ""
        try:
            event = json.loads(data_str)
            if isinstance(event, dict) and "incident" in event:
                return GcpmonitoringProvider._format_alert(event, self)
        except Exception:
            event = None

        labels = {k.lower(): v for k, v in (message.attributes or {}).items()}
        status = AlertStatus(labels.get("status", "firing"))
        severity = AlertSeverity[labels.get("severity", "info").upper()]
        event_time = (
            message.publish_time.isoformat()
            if message.publish_time
            else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        return AlertDto(
            id=message.message_id,
            name=labels.get("name", data_str),
            description=labels.get("description", data_str),
            message=data_str,
            status=status,
            severity=severity,
            lastReceived=event_time,
            firingStartTime=event_time,
            labels=labels,
            source=["gcppubsub"],
        )

    def start_consume(self):
        self.consume = True
        streaming_future = self.client.subscribe(
            self._subscription_path, callback=self._callback
        )
        self.logger.info(f"Listening for messages on {self._subscription_path}")
        while self.consume:
            time.sleep(1)
        streaming_future.cancel()

    def _callback(self, message):
        self.logger.info(f"Received message {message.message_id}")
        try:
            alert = self._message_to_alert(message)
            self._push_alert(alert.dict())
        except Exception as e:
            self.logger.error(f"Failed processing message: {e}")
        finally:
            message.ack()

    def stop_consume(self):
        self.consume = False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from keep.api.core.dependencies import SINGLE_TENANT_UUID

    context_manager = ContextManager(tenant_id=SINGLE_TENANT_UUID)
    config = {
        "authentication": {
            "project_id": "my-project",
            "subscription": "my-sub",
            "service_account_json": "{}",
        }
    }
    provider = ProvidersFactory.get_provider(
        context_manager,
        provider_id="gcppubsub-keephq",
        provider_type="gcppubsub",
        provider_config=config,
    )
    provider.start_consume()

