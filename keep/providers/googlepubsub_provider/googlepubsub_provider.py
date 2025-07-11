"""Google Pub/Sub Provider for Keep."""

from __future__ import annotations

import dataclasses
import json
import time
from typing import Optional

import google.api_core.exceptions
import pydantic
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from keep.api.models.alert import AlertDto
from keep.contextmanager.contextmanager import ContextManager
from keep.providers.base.base_provider import BaseProvider, ProviderHealthMixin
from keep.providers.gcpmonitoring_provider.gcpmonitoring_provider import (
    GcpmonitoringProvider,
)
from keep.providers.models.provider_config import ProviderConfig, ProviderScope


@pydantic.dataclasses.dataclass
class GooglepubsubProviderAuthConfig:
    """Google Pub/Sub authentication configuration."""

    service_account_json: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "A service account JSON with pubsub subscriber role",
            "sensitive": True,
            "type": "file",
            "name": "service_account_json",
            "file_type": "application/json",
        },
    )
    subscription: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Pub/Sub subscription name or full path",
            "hint": "projects/<project>/subscriptions/<name> or <name>",
        },
    )
    project_id: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Google Cloud project ID (required if subscription is not full path)",
        },
    )
    topic: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Pub/Sub topic name or full path used for publishing",
            "hint": "projects/<project>/topics/<name> or <name>",
        },
    )


class GooglepubsubProvider(BaseProvider, ProviderHealthMixin):
    """Consume alerts from Google Pub/Sub."""

    PROVIDER_DISPLAY_NAME = "Google Pub/Sub"
    PROVIDER_CATEGORY = ["Queues", "Cloud Infrastructure"]
    PROVIDER_SCOPES = [
        ProviderScope(
            name="roles/pubsub.subscriber",
            description="Read access to Pub/Sub subscriptions",
            mandatory=True,
            alias="Pub/Sub Subscriber",
        )
    ]
    PROVIDER_TAGS = ["queue"]

    def __init__(
        self, context_manager: ContextManager, provider_id: str, config: ProviderConfig
    ):
        super().__init__(context_manager, provider_id, config)
        self._client: Optional[pubsub_v1.SubscriberClient] = None
        self._publisher: Optional[pubsub_v1.PublisherClient] = None
        self.consume = False

    def dispose(self):
        pass

    def validate_config(self):
        self.authentication_config = GooglepubsubProviderAuthConfig(
            **self.config.authentication
        )
        sub = self.authentication_config.subscription
        if not sub.startswith("projects/"):
            if not self.authentication_config.project_id:
                raise ValueError(
                    "project_id is required when subscription is not full path"
                )
            self.subscription_path = (
                f"projects/{self.authentication_config.project_id}/subscriptions/{sub}"
            )
        else:
            self.subscription_path = sub

        topic = self.authentication_config.topic
        if topic:
            if not topic.startswith("projects/"):
                if not self.authentication_config.project_id:
                    raise ValueError(
                        "project_id is required when topic is not full path"
                    )
                self.topic_path = (
                    f"projects/{self.authentication_config.project_id}/topics/{topic}"
                )
            else:
                self.topic_path = topic
        else:
            self.topic_path = None

    def _get_client(self) -> pubsub_v1.SubscriberClient:
        if self._client is None:
            if self.authentication_config.service_account_json:
                info = json.loads(self.authentication_config.service_account_json)
                credentials = service_account.Credentials.from_service_account_info(
                    info
                )
                self._client = pubsub_v1.SubscriberClient(credentials=credentials)
            else:
                self._client = pubsub_v1.SubscriberClient()
        return self._client

    def _get_publisher(self) -> pubsub_v1.PublisherClient:
        if self._publisher is None:
            if self.authentication_config.service_account_json:
                info = json.loads(self.authentication_config.service_account_json)
                credentials = service_account.Credentials.from_service_account_info(
                    info
                )
                self._publisher = pubsub_v1.PublisherClient(credentials=credentials)
            else:
                self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    @property
    def client(self) -> pubsub_v1.SubscriberClient:
        return self._get_client()

    @property
    def publisher(self) -> pubsub_v1.PublisherClient:
        return self._get_publisher()

    def validate_scopes(self) -> dict[str, bool | str]:
        scopes = {"roles/pubsub.subscriber": False}
        try:
            self.client.pull(
                subscription=self.subscription_path, max_messages=1, timeout=5
            )
            scopes["roles/pubsub.subscriber"] = True
        except google.api_core.exceptions.GoogleAPIError as e:
            scopes["roles/pubsub.subscriber"] = str(e)
        return scopes

    def _notify(self, message: str, **attributes):
        if not self.topic_path:
            raise ValueError("topic not configured for Google Pub/Sub provider")
        data = str(message).encode("utf-8")
        future = self.publisher.publish(
            self.topic_path, data, **{k: str(v) for k, v in attributes.items()}
        )
        message_id = future.result()
        self.logger.info(
            "Published message to Pub/Sub", extra={"message_id": message_id}
        )
        return {"message_id": message_id}

    def _query(self, max_messages: int = 1, ack: bool = True) -> list[dict]:
        response = self.client.pull(
            subscription=self.subscription_path, max_messages=max_messages, timeout=5
        )
        messages = []
        ack_ids = []
        for received in response.received_messages:
            data = received.message.data.decode("utf-8")
            try:
                body = json.loads(data)
            except Exception:
                body = {"message": data}
            messages.append(body)
            ack_ids.append(received.ack_id)
        if ack and ack_ids:
            self.client.acknowledge(
                subscription=self.subscription_path, ack_ids=ack_ids
            )
        return messages

    def start_consume(self):
        self.consume = True
        while self.consume:
            try:
                response = self.client.pull(
                    subscription=self.subscription_path, max_messages=10, timeout=5
                )
            except google.api_core.exceptions.GoogleAPIError as e:
                self.logger.warning(
                    "Error pulling messages", extra={"exception": str(e)}
                )
                time.sleep(1)
                continue

            if not response.received_messages:
                time.sleep(1)
                continue

            ack_ids = []
            for received in response.received_messages:
                try:
                    data = received.message.data.decode("utf-8")
                    try:
                        event = json.loads(data)
                    except Exception:
                        event = {"message": data}
                    alert = self._format_alert(event)
                    self._push_alert(alert.dict())
                except Exception as e:
                    self.logger.exception(
                        "Failed to process message", extra={"exception": str(e)}
                    )
                finally:
                    ack_ids.append(received.ack_id)

            if ack_ids:
                try:
                    self.client.acknowledge(
                        subscription=self.subscription_path, ack_ids=ack_ids
                    )
                except google.api_core.exceptions.GoogleAPIError as e:
                    self.logger.warning(
                        "Failed to acknowledge messages", extra={"exception": str(e)}
                    )

    def stop_consume(self):
        self.consume = False

    @staticmethod
    def _format_alert(
        event: dict, provider_instance: BaseProvider | None = None
    ) -> AlertDto:
        return GcpmonitoringProvider._format_alert(event, provider_instance)
