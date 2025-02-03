from abc import ABC, abstractmethod
from typing import Any, Dict


class InterfaceNotifications(ABC):

    @abstractmethod
    def send_notification(self, notification_name: str, arguments: Dict[str, Any]) -> None:
        pass
