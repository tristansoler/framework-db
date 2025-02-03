from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.notification.interface_notifications import InterfaceNotifications
from typing import Any, Dict


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
