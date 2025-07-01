"""Google Pub/Sub provider for Keep."""

import dataclasses
import datetime
import json
import time
import typing

import pydantic
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from keep.api.models.alert import AlertDto, AlertSeverity, AlertStatus

from keep.contextmanager.contextmanager import ContextManager
from keep.providers.base.base_provider import BaseProvider
from keep.providers.models.provider_config import ProviderConfig, ProviderScope


@pydantic.dataclasses.dataclass
class GcppubsubProviderAuthConfig:
    """Authentication configuration for Google Pub/Sub."""

    service_account_json: typing.Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "The service account JSON with subscriber role",
            "sensitive": True,
            "type": "file",
            "name": "service_account_json",
            "file_type": "application/json",
        },
    )
    subscription: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Full Pub/Sub subscription path",
            "hint": "projects/<project-id>/subscriptions/<subscription>",
        }
    )


class GcppubsubProvider(BaseProvider):
    """Consume alerts from Google Pub/Sub."""

    PROVIDER_DISPLAY_NAME = "Google Pub/Sub"
    PROVIDER_CATEGORY = ["Queues", "Cloud Infrastructure"]
    PROVIDER_TAGS = ["queue"]

    PROVIDER_SCOPES = [
        ProviderScope(
            name="roles/pubsub.subscriber",
            description="Read access to Pub/Sub subscription",
            mandatory=True,
            alias="Subscriber",
        )
    ]

    def __init__(self, context_manager: ContextManager, provider_id: str, config: ProviderConfig) -> None:
        super().__init__(context_manager, provider_id, config)
        self.consume = False
        self._subscriber = None

    def validate_config(self) -> None:
        self.authentication_config = GcppubsubProviderAuthConfig(
            **(self.config.authentication or {})
        )

    def init_client(self) -> pubsub_v1.SubscriberClient:
        """Initialize and return a Pub/Sub subscriber client."""
        if self._subscriber is None:
            if self.authentication_config.service_account_json:
                # service account can be provided as dict or string
                if isinstance(self.authentication_config.service_account_json, dict):
                    info = self.authentication_config.service_account_json
                else:
                    info = json.loads(self.authentication_config.service_account_json)
                credentials = service_account.Credentials.from_service_account_info(info)
                self._subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            else:
                self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    @property
    def subscriber(self) -> pubsub_v1.SubscriberClient:
        return self.init_client()

    def dispose(self) -> None:
        if self._subscriber:
            self._subscriber.close()

    @staticmethod
    def _format_alert(
        event: dict, provider_instance: "BaseProvider" | None = None
    ) -> AlertDto | list[AlertDto]:
        """Map a Pub/Sub message to :class:`AlertDto`.

        If the message looks like a GCP Monitoring notification, reuse the
        GCP Monitoring provider formatter. Otherwise create a minimal alert.
        """
        if "incident" in event:
            from keep.providers.gcpmonitoring_provider.gcpmonitoring_provider import (
                GcpmonitoringProvider,
            )

            return GcpmonitoringProvider._format_alert(event)

        return AlertDto(
            name=event.get("name", "pubsub-message"),
            description=event.get("message")
            or json.dumps(event)[:200],
            status=AlertStatus.FIRING,
            severity=AlertSeverity.INFO,
            lastReceived=datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(),
            source=["gcppubsub"],
            payload=event,
        )

    def start_consume(self) -> None:
        self.consume = True
        subscription = self.authentication_config.subscription
        while self.consume:
            try:
                response = self.subscriber.pull(
                    subscription=subscription,
                    max_messages=10,
                    timeout=5,
                )
                ack_ids = []
                for received in response.received_messages:
                    ack_ids.append(received.ack_id)
                    data = received.message.data.decode("utf-8")
                    try:
                        payload = json.loads(data)
                    except Exception:
                        payload = {"message": data}
                    if received.message.attributes:
                        payload["attributes"] = dict(received.message.attributes)
                    alerts = self._format_alert(payload)
                    if not isinstance(alerts, list):
                        alerts = [alerts]
                    for alert in alerts:
                        self._push_alert(alert.dict())
                if ack_ids:
                    self.subscriber.acknowledge(
                        subscription=subscription, ack_ids=ack_ids
                    )
            except Exception as e:
                self.logger.error("Error pulling from Pub/Sub", exc_info=e)
                time.sleep(1)

    def stop_consume(self) -> None:
        self.consume = False

