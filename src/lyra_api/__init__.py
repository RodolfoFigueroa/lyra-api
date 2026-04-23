"""Lyra API Python bindings."""

from .api import AsyncLyraAPIClient, LyraAPIClient
from .exceptions import DownloadError, LyraAPIError, WebSocketError

__all__ = [
    "LyraAPIClient",
    "AsyncLyraAPIClient",
    "LyraAPIError",
    "WebSocketError",
    "DownloadError",
]


def main() -> None:
    """Entry point for the lyra-api CLI."""
    print("Hello from lyra-api!")
