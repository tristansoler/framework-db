from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import Environment
from data_framework.modules.notification.interface_notifications import (
    InterfaceNotifications,
    NotificationDict,
    NotificationType,
    NotificationToSend,
    Notification
)
from typing import Dict, Any, List


class Notifications(InterfaceNotifications):

    def __init__(self):
        self.config = config()
        self.notifications = self._combine_notifications()
        self.notifications_to_send = []

    def _combine_notifications(self) -> NotificationDict:
        default_notifications = self.config.data_framework_notifications.notifications
        custom_notifications = self.config.current_process_config().notifications
        default_keys = default_notifications.notifications.keys()
        custom_keys = custom_notifications.notifications.keys()
        common_keys = set(default_keys) & set(custom_keys)
        if common_keys:
            common_keys_str = ', '.join(common_keys)
            raise ValueError(
                f'The following notifications are already defined in Data Framework: {common_keys_str}. ' +
                'Please rename the notifications in your config file.'
            )
        else:
            combined_notifications = NotificationDict({
                **default_notifications.notifications,
                **custom_notifications.notifications
            })
            return combined_notifications

    def send_notification(self, notification_name: str, arguments: Dict[str, Any]) -> None:
        try:
            # Obtain notification info
            notification = self.notifications.get_notification(notification_name)
            if not notification.active:
                raise ValueError(f'Notification {notification_name} is deactivated')
            elif notification.type == NotificationType.EMAIL:
                self._send_email_notification(notification, notification_name, arguments)
            else:
                raise NotImplementedError(f'Notification type {notification.type.value} not implemented')
        except Exception as e:
            logger.error(f'Error sending notification {notification_name}: {e}')

    def _send_email_notification(
        self,
        notification: Notification,
        notification_name: str,
        arguments: Dict[str, Any]
    ) -> None:
        subject = self._format_subject(notification.subject, arguments)
        body = self._format_body(notification.body, arguments)
        valid_subject = self._validate_subject_length(subject, notification_name)
        valid_body = self._validate_body_length(body, notification_name)
        if valid_subject and valid_body:
            notification_to_send = NotificationToSend(
                type=notification.type.value,
                topics=[topic.value for topic in notification.topics],
                subject=subject,
                body=body
            )
            self._add_notification(notification_to_send)

    def _format_subject(self, subject: str, arguments: Dict[str, Any]) -> str:
        if self.config.environment == Environment.DEVELOP:
            return '[DEV]' + subject.format_map(arguments)
        elif self.config.environment == Environment.PREPRODUCTION:
            return '[PRE]' + subject.format_map(arguments)
        else:
            return subject.format_map(arguments)

    def _format_body(self, body: str, arguments: dict) -> str:
        signature = self.config.data_framework_notifications.parameters.signature
        if signature:
            return body.format_map(arguments) + signature
        else:
            return body.format_map(arguments)

    def _validate_subject_length(self, subject: str, notification_name: str) -> None:
        max_subject_len = self.config.data_framework_notifications.parameters.max_subject_length
        if len(subject) > max_subject_len:
            raise ValueError(
                f'Subject of the {notification_name} notifications exceeds the {max_subject_len} character limit'
            )

    def _validate_body_length(self, body: str, notification_name: str) -> None:
        max_body_len = self.config.data_framework_notifications.parameters.max_body_length
        if len(body) > max_body_len:
            raise ValueError(
                f'Body of the {notification_name} notifications exceeds the {max_body_len} character limit'
            )

    def _add_notification(self, notification_to_send: NotificationToSend) -> None:
        max_notifications = self.config.data_framework_notifications.parameters.max_number_of_notifications
        if len(self.notifications_to_send) < max_notifications:
            self.notifications_to_send.append(notification_to_send)
        else:
            raise ValueError(f'The limit of {max_notifications} notifications has been exceeded')

    def get_notifications_to_send(self) -> List[NotificationToSend]:
        return self.notifications_to_send
