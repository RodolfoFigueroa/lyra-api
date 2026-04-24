"""Lyra API client for processing data via WebSocket and downloading results via REST."""

import json
from typing import Any

import aiohttp
import requests
from websockets.asyncio.client import connect as async_connect
from websockets.sync.client import connect

from .exceptions import DownloadError, WebSocketError


class LyraAPIClient:
    """Synchronous client for interacting with the Lyra API.

    This client handles two-step data processing:
    1. Submit a processing request via WebSocket and receive a download ID
    2. Download the processed data via HTTP GET using the download ID

    Attributes:
        host: The API server hostname.
        timeout: Request timeout in seconds.
        headers: Default HTTP headers to include in all requests.
        secure: Whether to use secure protocols (https/wss) or insecure (http/ws).
        verbose: Whether to print status messages during processing.
    """

    def __init__(
        self,
        host: str,
        timeout: float = 100.0,
        headers: dict[str, str] | None = None,
        secure: bool = True,
        verbose: bool = True,
    ):
        """Initialize the Lyra API client.

        Args:
            host: The API server hostname.
            timeout: Request timeout in seconds. Defaults to 100.0.
            headers: Default HTTP headers to include in WebSocket and HTTP requests.
                If None, defaults to an empty dict.
            secure: Whether to use secure protocols (https/wss). Defaults to True.
            verbose: Whether to print status messages. Defaults to True.
        """
        self.host = host
        self.timeout = timeout
        self.headers = headers or {}
        self.secure = secure
        self.verbose = verbose

    def submit(self, metric: str, payload: dict) -> str:
        """Submit a processing request via WebSocket.

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.

        Returns:
            The download ID for retrieving the processed result.

        Raises:
            WebSocketError: If the WebSocket connection fails or the server
                returns an error status.
        """
        protocol = "wss" if self.secure else "ws"
        ws_url = f"{protocol}://{self.host}/ws/{metric}/geojson"

        try:
            with connect(ws_url, additional_headers=self.headers) as websocket:
                websocket.send(json.dumps(payload))

                # Receive acknowledgment
                ack_str = websocket.recv()
                ack = json.loads(ack_str)

                if ack["status"] == "error":
                    msg = ack.get("details", "Unknown error")
                    raise WebSocketError(f"Server error: {msg}")

                if self.verbose:
                    print(f"Server acknowledged. Task ID: {ack.get('task_id')}")

                # Receive processing result
                notification_str = websocket.recv()
                notification = json.loads(notification_str)
                status = notification["status"]

                if status == "error":
                    message = notification.get("message", "Unknown error")
                    raise WebSocketError(f"Worker failed: {message}")

                if status == "success":
                    download_id = notification.get("download_id")
                    if self.verbose:
                        print(
                            f"Worker finished. Received download ticket: {download_id}"
                        )
                    return download_id

                raise WebSocketError(f"Unexpected status: {status}")

        except WebSocketError:
            raise
        except Exception as e:
            raise WebSocketError(f"WebSocket error: {e}") from e

    def download(self, download_id: str) -> dict[str, Any]:
        """Download processed data via HTTP GET.

        Args:
            download_id: The download ID received from submit().

        Returns:
            The downloaded data as a dictionary.

        Raises:
            DownloadError: If the HTTP request fails.
        """
        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        try:
            response = requests.get(
                download_url,
                timeout=self.timeout,
                headers=self.headers,
            )

            if response.status_code == 200:
                return response.json()

            raise DownloadError(f"Failed to download data. HTTP {response.status_code}")

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Download error: {e}") from e

    def get_data_types(self) -> list[dict[str, Any]]:
        """Fetch available data types from the API.

        Returns:
            A list of data type objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"
        data_types_url = f"{protocol}://{self.host}/data_types"

        try:
            response = requests.get(
                data_types_url,
                timeout=self.timeout,
                headers=self.headers,
            )

            if response.status_code != 200:
                raise DownloadError(
                    f"Failed to fetch data types. HTTP {response.status_code}"
                )

            data_types = response.json()
            if not isinstance(data_types, list) or not all(
                isinstance(item, dict) for item in data_types
            ):
                raise DownloadError("Invalid data types response format")

            return data_types

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Data types request error: {e}") from e

    def get_metrics(self) -> list[dict[str, Any]]:
        """Fetch available metrics from the API.

        Returns:
            A list of metric objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"
        metrics_url = f"{protocol}://{self.host}/metrics"

        try:
            response = requests.get(
                metrics_url,
                timeout=self.timeout,
                headers=self.headers,
            )

            if response.status_code != 200:
                raise DownloadError(
                    f"Failed to fetch metrics. HTTP {response.status_code}"
                )

            metrics = response.json()
            if not isinstance(metrics, list) or not all(
                isinstance(item, dict) for item in metrics
            ):
                raise DownloadError("Invalid metrics response format")

            return metrics

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Metrics request error: {e}") from e

    def process(self, metric: str, payload: dict) -> dict[str, Any]:
        """Submit a request and download the result in one call.

        This is a convenience method combining submit() and download().

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.

        Returns:
            The processed data as a dictionary.

        Raises:
            WebSocketError: If the submission step fails.
            DownloadError: If the download step fails.
        """
        if self.verbose:
            print("Submitting processing request...")
        download_id = self.submit(metric, payload)

        if self.verbose:
            print("Downloading data via HTTP...")
        data = self.download(download_id)

        return data


class AsyncLyraAPIClient:
    """Asynchronous client for interacting with the Lyra API.

    This client handles two-step data processing with async/await support:
    1. Submit a processing request via WebSocket and receive a download ID
    2. Download the processed data via HTTP GET using the download ID

    Attributes:
        host: The API server hostname.
        timeout: Request timeout in seconds.
        headers: Default HTTP headers to include in all requests.
        secure: Whether to use secure protocols (https/wss) or insecure (http/ws).
        verbose: Whether to print status messages during processing.
    """

    def __init__(
        self,
        host: str,
        timeout: float = 100.0,
        headers: dict[str, str] | None = None,
        secure: bool = True,
        verbose: bool = True,
    ):
        """Initialize the async Lyra API client.

        Args:
            host: The API server hostname.
            timeout: Request timeout in seconds. Defaults to 100.0.
            headers: Default HTTP headers to include in WebSocket and HTTP requests.
                If None, defaults to an empty dict.
            secure: Whether to use secure protocols (https/wss). Defaults to True.
            verbose: Whether to print status messages. Defaults to True.
        """
        self.host = host
        self.timeout = timeout
        self.headers = headers or {}
        self.secure = secure
        self.verbose = verbose

    async def submit(self, metric: str, payload: dict) -> str:
        """Submit a processing request via WebSocket (async).

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.

        Returns:
            The download ID for retrieving the processed result.

        Raises:
            WebSocketError: If the WebSocket connection fails or the server
                returns an error status.
        """
        protocol = "wss" if self.secure else "ws"
        ws_url = f"{protocol}://{self.host}/ws/{metric}/geojson"

        try:
            async with async_connect(
                ws_url, additional_headers=self.headers
            ) as websocket:
                await websocket.send(json.dumps(payload))

                # Receive acknowledgment
                ack_str = await websocket.recv()
                ack = json.loads(ack_str)
                if self.verbose:
                    print(f"Server acknowledged. Task ID: {ack.get('task_id')}")

                # Receive processing result
                notification_str = await websocket.recv()
                notification = json.loads(notification_str)
                status = notification["status"]

                if status == "error":
                    message = notification.get("message", "Unknown error")
                    raise WebSocketError(f"Worker failed: {message}")

                if status == "success":
                    download_id = notification.get("download_id")
                    if self.verbose:
                        print(
                            f"Worker finished. Received download ticket: {download_id}"
                        )
                    return download_id

                raise WebSocketError(f"Unexpected status: {status}")

        except WebSocketError:
            raise
        except Exception as e:
            raise WebSocketError(f"WebSocket error: {e}") from e

    async def download(self, download_id: str) -> dict[str, Any]:
        """Download processed data via HTTP GET (async).

        Args:
            download_id: The download ID received from submit().

        Returns:
            The downloaded data as a dictionary.

        Raises:
            DownloadError: If the HTTP request fails.
        """
        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    download_url,
                    headers=self.headers,
                ) as response:
                    if response.status == 200:
                        return await response.json()

                    raise DownloadError(
                        f"Failed to download data. HTTP {response.status}"
                    )

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Download error: {e}") from e

    async def get_data_types(self) -> list[dict[str, Any]]:
        """Fetch available data types from the API (async).

        Returns:
            A list of data type objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"
        data_types_url = f"{protocol}://{self.host}/data_types"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    data_types_url,
                    headers=self.headers,
                ) as response:
                    if response.status != 200:
                        raise DownloadError(
                            f"Failed to fetch data types. HTTP {response.status}"
                        )

                    data_types = await response.json()
                    if not isinstance(data_types, list) or not all(
                        isinstance(item, dict) for item in data_types
                    ):
                        raise DownloadError("Invalid data types response format")

                    return data_types

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Data types request error: {e}") from e

    async def get_metrics(self) -> list[dict[str, Any]]:
        """Fetch available metrics from the API (async).

        Returns:
            A list of metric objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"
        metrics_url = f"{protocol}://{self.host}/metrics"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    metrics_url,
                    headers=self.headers,
                ) as response:
                    if response.status != 200:
                        raise DownloadError(
                            f"Failed to fetch metrics. HTTP {response.status}"
                        )

                    metrics = await response.json()
                    if not isinstance(metrics, list) or not all(
                        isinstance(item, dict) for item in metrics
                    ):
                        raise DownloadError("Invalid metrics response format")

                    return metrics

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Metrics request error: {e}") from e

    async def process(self, metric: str, payload: dict) -> dict[str, Any]:
        """Submit a request and download the result in one call (async).

        This is a convenience method combining submit() and download().

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.

        Returns:
            The processed data as a dictionary.

        Raises:
            WebSocketError: If the submission step fails.
            DownloadError: If the download step fails.
        """
        if self.verbose:
            print("Submitting processing request...")
        download_id = await self.submit(metric, payload)

        if self.verbose:
            print("Downloading data via HTTP...")
        data = await self.download(download_id)

        return data
