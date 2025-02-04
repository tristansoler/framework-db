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
            raise ValueError(f'Notification key {notification_name} not found. No notifications defined')
        notification = self.notifications.get(notification_name)
        if not notification:
            notification_names = ', '.join(self.notifications.keys())
            raise ValueError(
                f'Notification key {notification_name} not found. Available notification keys: {notification_names}'
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
