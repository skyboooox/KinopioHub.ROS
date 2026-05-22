"""Project-specific error types."""


class KinopioHubRosError(Exception):
    """Base class for project-specific failures."""


class ConfigError(KinopioHubRosError):
    """Raised when configuration is missing or invalid."""

    def __init__(self, message, field=None):
        self.field = field
        prefix = "{0}: ".format(field) if field else ""
        super().__init__(prefix + message)


class RuntimeUnavailableError(KinopioHubRosError):
    """Raised when a requested runtime path is not implemented yet."""


class ProtocolError(KinopioHubRosError):
    """Raised when a wire payload, subject, or topic is invalid."""


class AdapterError(KinopioHubRosError):
    """Raised when an external adapter cannot be configured or used."""


class ServiceCallError(AdapterError):
    """Raised when a ROS service call cannot complete."""

    def __init__(self, message, code="service_error"):
        self.code = code
        super().__init__(message)
