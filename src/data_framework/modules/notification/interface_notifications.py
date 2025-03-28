from data_framework.modules.exception.notification_exceptions import NotificationNotFoundError
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union
from dataclasses import dataclass, field
from enum import Enum


class NotificationType(Enum):
    EMAIL = "email"


class Topic(Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


@dataclass
class Notification:
    type: NotificationType
    topics: List[Topic]
    subject: str
    body: str
    active: bool = True


@dataclass
class NotificationToSend:
    type: str
    topics: List[str]
    subject: str
    body: str


@dataclass
class NotificationDict:
    notifications: Dict[str, Notification] = field(default_factory=dict)

    def get_notification(self, notification_name: str) -> Union[Notification, None]:
        if not self.notifications:
            raise NotificationNotFoundError(notification=notification_name)
        notification = self.notifications.get(notification_name)
        if not notification:
            raise NotificationNotFoundError(
                notification=notification_name,
                available_notifications=list(self.notifications.keys())
            )
        return notification


@dataclass
class NotificationsParameters:
    max_subject_length: int
    max_body_length: int
    max_number_of_notifications: int
    signature: str


@dataclass
class DataFrameworkNotifications:
    notifications: NotificationDict
    parameters: NotificationsParameters


class InterfaceNotifications(ABC):

    @abstractmethod
    def send_notification(self, notification_name: str, arguments: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_notifications_to_send(self) -> List[Notification]:
        pass
