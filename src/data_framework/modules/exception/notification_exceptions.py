"""Definition of exceptions for notification module"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class NotificationNotFoundError(DataFrameworkError):
    """Error raised when the system tries to send a notification that is not defined"""

    def __init__(self, notification: str, available_notifications: List[str] = None):
        error_message = f'Notification key {notification} not found'
        if available_notifications:
            available_notifications = ', '.join(available_notifications)
            error_message += f'. Available notification keys: {available_notifications}'
        else:
            error_message += '. No notifications defined'
        super().__init__(error_message)


class DuplicatedNotificationError(DataFrameworkError):
    """Error raised when the user defines a notification with
    the same name as one of the pre-defined notifications"""

    def __init__(self, duplicated_notifications: List[str]):
        duplicated_notifications = ', '.join(duplicated_notifications)
        super().__init__(
            f'The following notifications are already defined in Data Framework: {duplicated_notifications}. ' +
            'Please rename the notifications in your config file'
        )


class NotificationError(DataFrameworkError):
    """Error raised when a notification could not be sent successfully"""

    def __init__(self, notification: str):
        super().__init__(f'Error sending notification {notification}')


class SubjectLimitExceededError(DataFrameworkError):
    """Error raised when the maximum length of a notification subject is exceeded"""

    def __init__(self, notification: str, max_length: int):
        super().__init__(
            f'Subject of the {notification} notification exceeds the {max_length} character limit'
        )


class BodyLimitExceededError(DataFrameworkError):
    """Error raised when the maximum length of a notification body is exceeded"""

    def __init__(self, notification: str, max_length: int):
        super().__init__(
            f'Body of the {notification} notification exceeds the {max_length} character limit'
        )


class NotificationLimitExceededError(DataFrameworkError):
    """Error raised when the maximum number of notifications is exceeded"""

    def __init__(self, max_notifications: int):
        super().__init__(f'The limit of {max_notifications} notifications has been exceeded')
