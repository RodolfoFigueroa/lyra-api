"""Lyra API Python bindings."""

from .api import AsyncLyraAPIClient, LyraAPIClient
from .exceptions import DownloadError, LyraAPIError, WebSocketError

__all__ = [
    "AsyncLyraAPIClient",
    "DownloadError",
    "LyraAPIClient",
    "LyraAPIError",
    "WebSocketError",
]


def main() -> None:
    """Entry point for the lyra-api CLI."""
    print("Hello from lyra-api!")
