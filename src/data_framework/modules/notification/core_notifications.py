from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.notification.interface_notifications import InterfaceNotifications
from data_framework.modules.config.model.flows import Notification
from typing import Any, Dict, List


class CoreNotifications(object):

    @LazyClassProperty
    def _notifications(cls) -> InterfaceNotifications:
        from data_framework.modules.notification.notifications import Notifications
        return Notifications()

    @classmethod
    def send_notification(cls, notification_name: str, arguments: Dict[str, Any]) -> None:
        return cls._notifications.send_notification(
            notification_name=notification_name,
            arguments=arguments
        )

    @classmethod
    def get_notifications_to_send(cls) -> List[Notification]:
        return cls._notifications.get_notifications_to_send()
