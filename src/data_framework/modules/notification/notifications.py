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
from data_framework.modules.exception.notification_exceptions import (
    DuplicatedNotificationError,
    NotificationError,
    NotificationLimitExceededError,
    SubjectLimitExceededError,
    BodyLimitExceededError
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
            raise DuplicatedNotificationError(duplicated_notifications=list(common_keys))
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
                logger.warning(f'Notification {notification_name} is deactivated')
            else:
                if notification.type == NotificationType.EMAIL:
                    self._send_email_notification(notification, notification_name, arguments)
                else:
                    raise NotImplementedError(f'Notification type {notification.type.value} not implemented')
                logger.info(f'Notification {notification_name} sent successfully')
        except Exception:
            raise NotificationError(notification=notification_name)

    def _send_email_notification(
        self,
        notification: Notification,
        notification_name: str,
        arguments: Dict[str, Any]
    ) -> None:
        subject = self._format_subject(notification.subject, arguments)
        body = self._format_body(notification.body, arguments)
        self._validate_subject_length(subject, notification_name)
        self._validate_body_length(body, notification_name)
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
            raise SubjectLimitExceededError(notification=notification_name, max_length=max_subject_len)

    def _validate_body_length(self, body: str, notification_name: str) -> None:
        max_body_len = self.config.data_framework_notifications.parameters.max_body_length
        if len(body) > max_body_len:
            raise BodyLimitExceededError(notification=notification_name, max_length=max_body_len)

    def _add_notification(self, notification_to_send: NotificationToSend) -> None:
        max_notifications = self.config.data_framework_notifications.parameters.max_number_of_notifications
        if len(self.notifications_to_send) < max_notifications:
            self.notifications_to_send.append(notification_to_send)
        else:
            raise NotificationLimitExceededError(max_notifications=max_notifications)

    def get_notifications_to_send(self) -> List[NotificationToSend]:
        return self.notifications_to_send
