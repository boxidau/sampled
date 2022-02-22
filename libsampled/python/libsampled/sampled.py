from typing import Dict, TypedDict, Union, List, Optional

import time
import threading
import socket
import os
import json
import queue

# flush sample buffer to daemon when either size or interval is reached
SAMPLED_BUFFER_FLUSH_SIZE = int(os.getenv("SAMPLED_BUFFER_FLUSH_SIZE", 100))
SAMPLED_BUFFER_FLUSH_INTERVAL_MS = int(
    os.getenv("SAMPLED_BUFFER_FLUSH_INTERVAL_MS", 1000)
)

SAMPLED_DAEMON_HOST = os.getenv("SAMPLED_DAEMON_HOST", "localhost")
SAMPLED_DAEMON_PORT = int(os.getenv("SAMPLED_DAEMON_PORT", 7675))

FieldType = Union[str, float, int, List[str]]

SampleType = Dict[str, FieldType]


class QueueItem(TypedDict):
    timestamp: int
    dataset: str
    sample: SampleType


def _now_milliseconds() -> int:
    return int(time.time() * 1000)


class SampleBuffer:
    def __init__(self) -> None:
        self.__q: queue.Queue[QueueItem] = queue.Queue(maxsize=1000)
        self.__buffer = b""
        self.__buffer_message_count = 0
        self.__last_flush = _now_milliseconds()
        self.__socket: Optional[socket.socket] = None

        sampled_thread = threading.Thread(
            target=self._flush_loop, name="SampledPublisher", daemon=True
        )
        sampled_thread.start()

    def _flush_loop(self) -> None:
        while True:
            try:
                item = self.__q.get(timeout=0.05)
                self.__buffer += json.dumps(item).replace("\r", " ").encode()
                self.__buffer += b"\r"
                self.__buffer_message_count += 1
                self.__q.task_done()
            except queue.Empty:
                pass

            if self.__buffer_message_count == 0:
                continue

            at_flush_size = self.__buffer_message_count >= SAMPLED_BUFFER_FLUSH_SIZE
            at_flush_time = _now_milliseconds() >= (
                self.__last_flush + SAMPLED_BUFFER_FLUSH_INTERVAL_MS
            )

            if at_flush_size or at_flush_time:
                self._do_flush()

    def _do_flush(self) -> None:
        buffer, self.__buffer = self.__buffer, b""
        self.__buffer_message_count = 0
        self.__last_flush = _now_milliseconds()

        attempt = 1
        while attempt <= 3:
            try:
                if not self.__socket:
                    self.__socket = socket.create_connection(
                        (SAMPLED_DAEMON_HOST, SAMPLED_DAEMON_PORT)
                    )
                self.__socket.sendall(buffer)
                return
            except Exception as e:
                if self.__socket is not None:
                    self.__socket.close()
                    self.__socket = None
                time.sleep(1)
                attempt += 1

    def add_sample(self, dataset: str, sample: Dict[str, FieldType]) -> None:
        try:
            self.__q.put(
                item={
                    "timestamp": _now_milliseconds(),
                    "dataset": dataset,
                    "sample": sample,
                },
                block=False,
            )
        except queue.Full:
            # drop incoming samples if the queue is full
            # a full queue means the publisher thread is broken
            pass


_sample_buffer = SampleBuffer()


def add_sample(dataset: str, sample: Dict[str, FieldType]) -> None:
    _sample_buffer.add_sample(dataset, sample)
