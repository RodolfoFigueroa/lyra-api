"""Custom exceptions for Lyra API client."""


class LyraAPIError(Exception):
    """Base exception for all Lyra API errors."""

    pass


class WebSocketError(LyraAPIError):
    """Exception raised for WebSocket-related errors."""

    pass


class DownloadError(LyraAPIError):
    """Exception raised for download/HTTP-related errors."""

    pass
