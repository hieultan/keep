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
    subscription: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Pub/Sub subscription path (projects/<project>/subscriptions/<name>)",
        }
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
        self._service_account_data = json.loads(
            self.authentication_config.service_account_json
        )
        self._client: pubsub_v1.SubscriberClient | None = None
        self.consume = False
        
    def dispose(self):
        if self._client:
            self._client.close()

    def validate_config(self):
        if self.config.authentication is None:
            self.config.authentication = {}
        self.authentication_config = GcppubsubProviderAuthConfig(
            **self.config.authentication
        )

        # def _generate_client(self) -> pubsub_v1.SubscriberClient:
        # if not self._client:
        #     if self.authentication_config.service_account_json:
        #         info = json.loads(self.authentication_config.service_account_json)
        #         self._client = pubsub_v1.SubscriberClient.from_service_account_info(
        #             info
        #         )
        #     else:
        #         self._client = pubsub_v1.SubscriberClient()
        # return self._client
    def _generate_client(self) -> pubsub_v1.SubscriberClient:
        if not self._client:
            self._client = pubsub_v1.SubscriberClient.from_service_account_info(
                self._service_account_data
            )
        return self._client
    @property
    def client(self) -> pubsub_v1.SubscriberClient:
        return self._generate_client()


    def _message_to_alert(self, message) -> AlertDto:
        data_str = message.data.decode("utf-8") if message.data else ""
        self.logger.debug(f"Parsing message {message.message_id}: {data_str}")
        try:
            event = json.loads(data_str)
            if isinstance(event, dict) and "incident" in event:
                self.logger.debug(f"Message {message.message_id} recognized as GCP Monitoring alert.")
                return GcpmonitoringProvider._format_alert(event, self)
        except Exception as e:
            self.logger.debug(f"Message {message.message_id} is not a GCP Monitoring alert: {e}")
            event = None

        labels = {k.lower(): v for k, v in (message.attributes or {}).items()}
        self.logger.debug(f"Message {message.message_id} attributes: {labels}")
        status = AlertStatus(labels.get("state", data_str))
        severity = AlertSeverity[labels.get("severity", data_str).upper()]
        event_time = (
            message.publish_time.isoformat()
            if message.publish_time
            else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        alert = AlertDto(
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
        self.logger.debug(f"AlertDto created from message {message.message_id}: {alert}")
        return alert

    def start_consume(self):
        """
        Use streaming pull with callback for message ack, instead of synchronous pull/ack loop.
        """
        self.consume = True
        client = self._generate_client()
        subscription = self.authentication_config.subscription
        self.logger.info(f"Starting streaming pull on subscription {subscription}")
        streaming_future = client.subscribe(subscription, callback=self._callback)
        try:
            while self.consume:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user, stopping consumer.")
        finally:
            streaming_future.cancel()
            self.logger.info("Streaming pull cancelled.")

    def _callback(self, message):
        self.logger.info(f"Received message {message.message_id}")
        try:
            alert = self._message_to_alert(message)
            self.logger.debug(f"Pushing alert for message {message.message_id}")
            self._push_alert(alert.dict())
        except Exception as e:
            self.logger.error(f"Failed processing message {message.message_id}: {e}")
        finally:
            self.logger.debug(f"Acknowledging message {message.message_id}")
            message.ack()

    def stop_consume(self):
        self.consume = False
        self.logger.info("Stopped consuming messages.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from keep.api.core.dependencies import SINGLE_TENANT_UUID

    context_manager = ContextManager(tenant_id=SINGLE_TENANT_UUID)
    config = {
        "authentication": {
            "subscription": "",
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

