import threading
import queue
from enum import StrEnum
from time import sleep
from typing import Callable, Optional


class RunnerMode(StrEnum):
    """Runner mode for local or cloud execution."""

    LOCAL = "local"
    CLOUD = "cloud"


class ExternalSystem:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._call_queue = queue.Queue()
        self._worker_thread = None
        self._shutdown_flag = threading.Event()

        self._mode = RunnerMode.CLOUD
        self._success_handler = self._cloud_success_handler
        self._failure_handler = self._cloud_failure_handler
        self._heartbeat_handler = self._cloud_heartbeat_handler
        self._initialized = True

    @property
    def mode(self) -> RunnerMode:
        return self._mode

    def activate_local_mode(
        self,
        success_handler: Optional[Callable[[str, bytes], None]] = None,
        failure_handler: Optional[Callable[[str, Exception], None]] = None,
        heartbeat_handler: Optional[Callable[[str], None]] = None,
    ):
        """Activate local mode with custom handlers."""
        self._mode = RunnerMode.LOCAL
        self._success_handler = success_handler
        self._failure_handler = failure_handler
        self._heartbeat_handler = heartbeat_handler

    def activate_cloud_mode(self):
        """Activate cloud mode with boto3 handlers."""
        self._mode = RunnerMode.CLOUD
        self._success_handler = self._cloud_success_handler
        self._failure_handler = self._cloud_failure_handler
        self._heartbeat_handler = self._cloud_heartbeat_handler

    def send_success(self, callback_id: str, msg: bytes):
        """Send success callback."""
        self._call_queue.put(("success", callback_id, msg), timeout=0.5)

    def send_failure(self, callback_id: str, error: Exception):
        """Send failure callback."""
        self._call_queue.put(("failure", callback_id, error), timeout=0.5)

    def send_heartbeat(self, callback_id: str):
        """Send heartbeat callback."""
        self._call_queue.put(("heartbeat", callback_id, None), timeout=0.5)

    def start(self):
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._worker_thread = threading.Thread(target=self._worker, daemon=True)
            self._worker_thread.start()

    def _worker(self):
        """Background worker that processes callbacks."""
        while not self._shutdown_flag.is_set():
            try:
                operation_type, callback_id, data = self._call_queue.get(timeout=0.5)

                if operation_type == "success" and self._success_handler:
                    self._success_handler(callback_id, data)
                elif operation_type == "failure" and self._failure_handler:
                    self._failure_handler(callback_id, data)
                elif operation_type == "heartbeat" and self._heartbeat_handler:
                    self._heartbeat_handler(callback_id)

                self._call_queue.task_done()
            except queue.Empty:
                continue

    def reset(self):
        """Reset the external system state."""
        # Clear the queue
        while not self._call_queue.empty():
            try:
                self._call_queue.get_nowait()
                self._call_queue.task_done()
            except queue.Empty:
                break

    def shutdown(self):
        """Shutdown the worker thread."""
        self._shutdown_flag.set()

        # Clear the queue
        while not self._call_queue.empty():
            try:
                self._call_queue.get_nowait()
                self._call_queue.task_done()
            except queue.Empty:
                break

        # Wait for thread to finish
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=1)

        # Reset for next test
        self._worker_thread = None
        self._shutdown_flag.clear()

    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance."""
        with cls._lock:
            if cls._instance:
                cls._instance.shutdown()
            cls._instance = None

    def _cloud_success_handler(self, callback_id: str, msg: bytes):
        """Default cloud success handler using boto3."""
        try:
            import boto3
            import os

            client = boto3.client(
                "lambdainternal",
                endpoint_url=os.environ.get("LAMBDA_ENDPOINT"),
                region_name=os.environ.get("AWS_REGION", "us-west-2"),
            )

            client.send_durable_execution_callback_success(
                CallbackId=callback_id, Result=msg.decode("utf-8") if msg else None
            )
        except Exception:
            pass  # Fail silently in cloud mode

    def _cloud_failure_handler(self, callback_id: str, error: Exception):
        """Default cloud failure handler using boto3."""
        try:
            import boto3
            import os

            client = boto3.client(
                "lambdainternal",
                endpoint_url=os.environ.get("LAMBDA_ENDPOINT"),
                region_name=os.environ.get("AWS_REGION", "us-west-2"),
            )

            client.send_durable_execution_callback_failure(
                CallbackId=callback_id, Error=str(error)
            )
        except Exception:
            pass  # Fail silently in cloud mode

    def _cloud_heartbeat_handler(self, callback_id: str):
        """Default cloud heartbeat handler using boto3."""
        try:
            import boto3
            import os

            client = boto3.client(
                "lambdainternal",
                endpoint_url=os.environ.get("LAMBDA_ENDPOINT"),
                region_name=os.environ.get("AWS_REGION", "us-west-2"),
            )

            client.send_durable_execution_callback_heartbeat(CallbackId=callback_id)
        except Exception:
            pass  # Fail silently in cloud mode
