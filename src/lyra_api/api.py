"""Lyra API client for processing data via WebSocket and downloading results via REST."""

import json
import logging
import os
from typing import Any, overload

import aiohttp
import requests
from websockets.asyncio.client import connect as async_connect
from websockets.sync.client import connect

from lyra_api.exceptions import DownloadError, WebSocketError


class _BaseLyraAPIClient:
    """Base class for Lyra API clients, containing shared logic and configuration."""

    def _validate_metric_response(
        self,
        response: list | dict,
        metric_name: str | None,
    ) -> None:
        """Validate the format of the metrics response.

        Args:
            response: The raw response data to validate.
            metric_name: The metric name used in the request, or None for all metrics.

        Raises:
            DownloadError: If the response format is invalid.
        """
        if metric_name is None:
            if not isinstance(response, list) or not all(
                isinstance(item, dict) for item in response
            ):
                err = "Invalid metrics response format"
                raise DownloadError(err)
        elif not isinstance(response, dict):
            err = "Invalid metric response format"
            raise DownloadError(err)


class LyraAPIClient(_BaseLyraAPIClient):
    """Synchronous client for interacting with the Lyra API.

    This client handles two-step data processing:
    1. Submit a processing request via WebSocket and receive a download ID
    2. Download the processed data via HTTP GET using the download ID

    Attributes:
        host: The API server hostname.
        timeout: Request timeout in seconds.
        headers: Default HTTP headers to include in all requests.
        secure: Whether to use secure protocols (https/wss) or insecure (http/ws).
        log_level: Logging level for status messages. Defaults to logging.INFO.
    """

    def __init__(
        self,
        host: str,
        timeout: float = 100.0,
        headers: dict[str, str] | None = None,
        *,
        secure: bool = True,
        log_level: int = logging.INFO,
        connect_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the Lyra API client.

        Args:
            host: The API server hostname.
            timeout: Request timeout in seconds. Defaults to 100.0.
            headers: Default HTTP headers to include in WebSocket and HTTP requests.
                If None, defaults to an empty dict.
            secure: Whether to use secure protocols (https/wss). Defaults to True.
            log_level: Logging level for status messages. Defaults to logging.INFO.
        """
        self.host = host
        self.timeout = timeout
        self.headers = headers or {}
        self.secure = secure
        self.connect_kwargs = connect_kwargs or {}
        self._logger = logging.getLogger(__name__ + ".LyraAPIClient")
        self._logger.setLevel(log_level)

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
        ws_url = f"{protocol}://{self.host}/ws/{metric}"

        try:
            with connect(
                ws_url,
                additional_headers=self.headers,
                **self.connect_kwargs,
            ) as websocket:
                websocket.send(json.dumps(payload))

                # Receive acknowledgment
                ack_str = websocket.recv()
                ack = json.loads(ack_str)

                if ack["status"] == "error":
                    message = ack.get("message", "Unknown error")
                    err = f"Server error: {message}"
                    raise WebSocketError(err)

                self._logger.info(
                    "Server acknowledged. Task ID: %s",
                    ack.get("task_id"),
                )

                # Receive processing result
                notification_str = websocket.recv()
                notification = json.loads(notification_str)
                status = notification["status"]

                if status == "error":
                    message = notification.get("message", "Unknown error")
                    raise WebSocketError(f"Worker failed: {message}")

                if status == "success":
                    download_id = notification.get("download_id")
                    self._logger.info(
                        "Worker finished. Received download ticket: %s",
                        download_id,
                    )
                    return download_id

                raise WebSocketError(f"Unexpected status: {status}")

        except WebSocketError:
            raise
        except Exception as e:
            err = f"WebSocket error: {e}"
            raise WebSocketError(err) from e

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

            err = f"Failed to download data. HTTP {response.status_code}"
            raise DownloadError(err)

        except DownloadError:
            raise
        except Exception as e:
            err = f"Download error: {e}"
            raise DownloadError(err) from e

    def download_to_file(self, download_id: str, path: str | os.PathLike[str]) -> None:
        """Download a result by ticket and write it to a file.

        Args:
            download_id: The download ID received from submit().
            path: The file path to write the result to.

        Raises:
            DownloadError: If the HTTP request fails.
        """
        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        try:
            with requests.get(
                download_url,
                timeout=self.timeout,
                headers=self.headers,
                stream=True,
            ) as response:
                if response.status_code == 200:
                    with open(path, "wb") as f:
                        f.writelines(response.iter_content(chunk_size=65536))
                    return

                err = f"Failed to download data. HTTP {response.status_code}"
                raise DownloadError(err)

        except DownloadError:
            raise
        except Exception as e:
            err = f"Download error: {e}"
            raise DownloadError(err) from e

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
                err = f"Failed to fetch data types. HTTP {response.status_code}"
                raise DownloadError(err)

            data_types = response.json()
            if not isinstance(data_types, list) or not all(
                isinstance(item, dict) for item in data_types
            ):
                err = "Invalid data types response format"
                raise DownloadError(err)

            return data_types

        except DownloadError:
            raise
        except Exception as e:
            err = f"Data types request error: {e}"
            raise DownloadError(err) from e

    @overload
    def get_metrics(self, metric_name: None) -> list[dict[str, Any]]: ...

    @overload
    def get_metrics(self, metric_name: str) -> dict[str, Any]: ...

    def get_metrics(
        self,
        metric_name: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Fetch available metrics from the API.

        Returns:
            A list of metric objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"

        metric_str = "" if metric_name is None else metric_name
        metrics_url = f"{protocol}://{self.host}/metrics/{metric_str}"

        try:
            response = requests.get(
                metrics_url,
                timeout=self.timeout,
                headers=self.headers,
            )

            if response.status_code != 200:
                err = f"Failed to fetch metrics. HTTP {response.status_code}"
                raise DownloadError(err)

            metrics = response.json()
            self._validate_metric_response(metrics, metric_name)
        except DownloadError:
            raise
        except Exception as e:
            err = f"Metrics request error: {e}"
            raise DownloadError(err) from e
        else:
            return metrics

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
        self._logger.info("Submitting processing request...")
        download_id = self.submit(metric, payload)

        self._logger.info("Downloading data via HTTP...")
        return self.download(download_id)

    def process_to_file(
        self,
        metric: str,
        payload: dict,
        path: str | os.PathLike[str],
    ) -> None:
        """Submit a request and write the result to a file.

        This is a convenience method combining submit() and a file download.
        Unlike process(), the result is written directly to disk rather than
        loaded into memory.

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.
            path: The file path to write the result to.

        Raises:
            WebSocketError: If the submission step fails.
            DownloadError: If the download step fails.
        """
        self._logger.info("Submitting processing request...")
        download_id = self.submit(metric, payload)

        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        self._logger.info("Downloading data to file...")
        try:
            with requests.get(
                download_url,
                timeout=self.timeout,
                headers=self.headers,
                stream=True,
            ) as response:
                if response.status_code == 200:
                    with open(path, "wb") as f:
                        f.writelines(response.iter_content(chunk_size=65536))
                    return

                err = f"Failed to download data. HTTP {response.status_code}"
                raise DownloadError(err)

        except DownloadError:
            raise
        except Exception as e:
            err = f"Download error: {e}"
            raise DownloadError(err) from e


class AsyncLyraAPIClient(_BaseLyraAPIClient):
    """Asynchronous client for interacting with the Lyra API.

    This client handles two-step data processing with async/await support:
    1. Submit a processing request via WebSocket and receive a download ID
    2. Download the processed data via HTTP GET using the download ID

    Attributes:
        host: The API server hostname.
        timeout: Request timeout in seconds.
        headers: Default HTTP headers to include in all requests.
        secure: Whether to use secure protocols (https/wss) or insecure (http/ws).
        log_level: Logging level for status messages. Defaults to logging.INFO.
    """

    def __init__(
        self,
        host: str,
        timeout: float = 100.0,
        headers: dict[str, str] | None = None,
        *,
        secure: bool = True,
        log_level: int = logging.INFO,
    ) -> None:
        """Initialize the async Lyra API client.

        Args:
            host: The API server hostname.
            timeout: Request timeout in seconds. Defaults to 100.0.
            headers: Default HTTP headers to include in WebSocket and HTTP requests.
                If None, defaults to an empty dict.
            secure: Whether to use secure protocols (https/wss). Defaults to True.
            log_level: Logging level for status messages. Defaults to logging.INFO.
        """
        self.host = host
        self.timeout = timeout
        self.headers = headers or {}
        self.secure = secure
        self._logger = logging.getLogger(__name__ + ".AsyncLyraAPIClient")
        self._logger.setLevel(log_level)

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
        ws_url = f"{protocol}://{self.host}/ws/{metric}"

        try:
            async with async_connect(
                ws_url,
                additional_headers=self.headers,
            ) as websocket:
                await websocket.send(json.dumps(payload))

                # Receive acknowledgment
                ack_str = await websocket.recv()
                ack = json.loads(ack_str)
                self._logger.info(
                    "Server acknowledged. Task ID: %s",
                    ack.get("task_id"),
                )

                # Receive processing result
                notification_str = await websocket.recv()
                notification = json.loads(notification_str)
                status = notification["status"]

                if status == "error":
                    err = (
                        f"Worker failed: {notification.get('message', 'Unknown error')}"
                    )
                    raise WebSocketError(err)

                if status == "success":
                    download_id = notification.get("download_id")
                    self._logger.info(
                        "Worker finished. Received download ticket: %s",
                        download_id,
                    )
                    return download_id

                err = f"Unexpected status: {status}"
                raise WebSocketError(err)

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
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    download_url,
                    headers=self.headers,
                ) as response,
            ):
                if response.status == 200:
                    return await response.json()

                err = f"Failed to download data. HTTP {response.status}"
                raise DownloadError(err)

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Download error: {e}") from e

    async def download_to_file(
        self,
        download_id: str,
        path: str | os.PathLike[str],
    ) -> None:
        """Download a result by ticket and write it to a file (async).

        Args:
            download_id: The download ID received from submit().
            path: The file path to write the result to.

        Raises:
            DownloadError: If the HTTP request fails.
        """
        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    download_url,
                    headers=self.headers,
                ) as response,
            ):
                if response.status == 200:
                    with open(path, "wb") as f:
                        async for chunk in response.content.iter_chunked(65536):
                            f.write(chunk)
                    return

                err = f"Failed to download data. HTTP {response.status}"
                raise DownloadError(err)

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
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    data_types_url,
                    headers=self.headers,
                ) as response,
            ):
                if response.status != 200:
                    err = f"Failed to fetch data types. HTTP {response.status}"
                    raise DownloadError(err)

                data_types = await response.json()
                if not isinstance(data_types, list) or not all(
                    isinstance(item, dict) for item in data_types
                ):
                    err = "Invalid data types response format"
                    raise DownloadError(err)

                return data_types

        except DownloadError:
            raise
        except Exception as e:
            err = f"Data types request error: {e}"
            raise DownloadError(err) from e

    @overload
    async def get_metrics(self, metric_name: None) -> list[dict[str, Any]]: ...

    @overload
    async def get_metrics(self, metric_name: str) -> dict[str, Any]: ...

    async def get_metrics(
        self,
        metric_name: str | None = None,
    ) -> list[dict[str, Any]] | dict:
        """Fetch available metrics from the API (async).

        Returns:
            A list of metric objects.

        Raises:
            DownloadError: If the HTTP request fails or returns an invalid payload.
        """
        protocol = "https" if self.secure else "http"
        metric_str = "" if metric_name is None else metric_name
        metrics_url = f"{protocol}://{self.host}/metrics/{metric_str}"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    metrics_url,
                    headers=self.headers,
                ) as response,
            ):
                if response.status != 200:
                    err = f"Failed to fetch metrics. HTTP {response.status}"
                    raise DownloadError(err)

                metrics = await response.json()
                self._validate_metric_response(metrics, metric_name)
        except DownloadError:
            raise
        except Exception as e:
            err = f"Metrics request error: {e}"
            raise DownloadError(err) from e
        else:
            return metrics

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
        self._logger.info("Checking metric...")
        response = await self.get_metrics(metric)

        self._logger.info("Submitting processing request...")
        download_id = await self.submit(metric, payload)

        self._logger.info("Downloading data via HTTP...")
        return await self.download(download_id)

    async def process_to_file(
        self,
        metric: str,
        payload: dict,
        path: str | os.PathLike[str],
    ) -> None:
        """Submit a request and write the result to a file (async).

        This is a convenience method combining submit() and a file download.
        Unlike process(), the result is written directly to disk rather than
        loaded into memory.

        Args:
            metric: The metric identifier for the processing task.
            payload: The data payload to process.
            path: The file path to write the result to.

        Raises:
            WebSocketError: If the submission step fails.
            DownloadError: If the download step fails.
        """
        self._logger.info("Submitting processing request...")
        download_id = await self.submit(metric, payload)

        protocol = "https" if self.secure else "http"
        download_url = f"{protocol}://{self.host}/download_result/{download_id}"

        self._logger.info("Downloading data to file...")
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    download_url,
                    headers=self.headers,
                ) as response,
            ):
                if response.status == 200:
                    with open(path, "wb") as f:
                        async for chunk in response.content.iter_chunked(65536):
                            f.write(chunk)
                    return

                err = f"Failed to download data. HTTP {response.status}"
                raise DownloadError(err)

        except DownloadError:
            raise
        except Exception as e:
            raise DownloadError(f"Download error: {e}") from e
