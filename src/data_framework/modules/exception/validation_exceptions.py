"""Definition of exceptions for validation module"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class QualityControlsError(DataFrameworkError):
    """Error raised when data validation fails"""

    def __init__(self, table_name: str):
        super().__init__(f'Error validating data from {table_name}')


class FailedRulesError(DataFrameworkError):
    """Error raised when one or more quality rules fail"""

    def __init__(self, n_failed_rules: int):
        super().__init__(f'There are {n_failed_rules} rules that could not be executed correctly')


class RuleComputeError(DataFrameworkError):
    """Error raised when a rule computation fails"""

    def __init__(self, rule_id: str, rule_type: str):
        super().__init__(f'The computation of rule {rule_id} of type {rule_type} has failed')


class ValidationFunctionNotFoundError(DataFrameworkError):
    """Error raised when the Python function defined for a validation is not found"""

    def __init__(self, function_name: str):
        super().__init__(
            f'Validation function {function_name} not found. ' +
            'Please check that the module or class that you have configure ' +
            'with QualityControls().set_parent() has this function defined'
        )


class ParentNotConfiguredError(DataFrameworkError):
    """Error raised when the parent with validation functions is not configured"""

    def __init__(self):
        super().__init__(
            'QualityControls parent is not set. Please define a module or class to serve ' +
            'as source of custom validation functions using QualityControls().set_parent()'
        )


class InvalidThresholdError(DataFrameworkError):
    """Error raised when the threshold defined for a rule is not valid"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class InvalidAlgorithmError(DataFrameworkError):
    """Error raised when the algorithm defined for a rule is not valid"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class InvalidRuleError(DataFrameworkError):
    """Error raised when the definition of a rule is not valid"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class InvalidDataFrameError(DataFrameworkError):
    """Error raised when the threshold defined for a rule is not valid"""

    def __init__(self, error_message: str):
        super().__init__(error_message)
