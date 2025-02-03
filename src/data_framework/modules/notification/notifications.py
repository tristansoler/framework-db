from data_framework.modules.notification.interface_notifications import InterfaceNotifications
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import (
    Environment,
    NotificationDict,
    Notification,
    NotificationType
)
from typing import Dict, Any
from copy import deepcopy


class Notifications(InterfaceNotifications):

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.notifications = self._combine_notifications(
            config().data_framework_notifications,
            config().current_process_config().notifications
        )
        self.notifications_to_send = []

    @staticmethod
    def _combine_notifications(
            data_framework_notifications: NotificationDict,
            custom_notifications: NotificationDict
    ) -> NotificationDict:
        df_keys = data_framework_notifications.notifications.keys()
        custom_keys = custom_notifications.notifications.keys()
        common_keys = set(df_keys) & set(custom_keys)
        if common_keys:
            common_keys_str = ', '.join(common_keys)
            raise ValueError(
                f'The following notifications are already defined in Data Framework: {common_keys_str}. ' +
                'Please rename the notifications in your config file.'
            )
        else:
            combined_notifications = NotificationDict({
                **data_framework_notifications.notifications,
                **custom_notifications.notifications
            })
            return combined_notifications

    def send_notification(self, notification_name: str, arguments: Dict[str, Any]) -> None:
        # Obtain notification info
        notification = self.notifications.get_notification(notification_name)
        if notification.type == NotificationType.EMAIL:
            notification_to_send = deepcopy(notification)
            notification_to_send.format_subject(arguments, self.config.environment)
            notification_to_send.validate_subject_length(notification_name)
            notification_to_send.format_body(arguments)
            notification_to_send.validate_body_length(notification_name)
            # TODO: ver si mover a otro sitio el cambio de type y topics
            notification_to_send.type = notification_to_send.type.value
            notification_to_send.topics = [topic.value for topic in notification_to_send.topics]
            self._add_notification(notification_to_send)
            # TODO: ¿parametrizar longitud máxima y número de notificaciones máximo en el fichero config?
        else:
            raise NotImplementedError(f'Notification type {notification.type.value} not implemented')

    def _add_notification(self, notification_to_send: Notification, max_notifications: int = 5) -> None:
        if len(self.notifications_to_send) < max_notifications:
            self.notifications_to_send.append(notification_to_send)
        else:
            raise ValueError(f'The limit of {max_notifications} notifications has been exceeded')
