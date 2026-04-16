"""Custom exceptions for Delta."""


class DeltaError(Exception):
    """Base exception for all Delta errors."""


class ConfigError(DeltaError):
    """Configuration-related error."""


class StorageError(DeltaError):
    """Storage or filesystem error."""


class ConnectionError(DeltaError):
    """SSH connection error."""


class NameConflictError(DeltaError):
    """Name already exists in the unified namespace."""


class NotFoundError(DeltaError):
    """Requested baseline or patch not found."""


class ValidationError(DeltaError):
    """Input validation error."""


class RemoteCommandError(DeltaError):
    """Remote command execution failed."""


class AbortedError(DeltaError):
    """Operation aborted by user."""
