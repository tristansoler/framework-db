from data_framework.modules.config.model.flows import Notification
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class InterfaceNotifications(ABC):

    @abstractmethod
    def send_notification(self, notification_name: str, arguments: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_notifications_to_send(self) -> List[Notification]:
        pass
