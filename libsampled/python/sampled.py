from typing import Dict, Union, List

import time
import threading
import socket
import os
import json

# flush sample buffer to daemon when either size or interval is reached
SAMPLED_BUFFER_FLUSH_SIZE = int(os.getenv("SAMPLED_BUFFER_FLUSH_SIZE", 10))
SAMPLED_BUFFER_FLUSH_INTERVAL_MS = int(
    os.getenv("SAMPLED_BUFFER_FLUSH_INTERVAL_MS", 1000)
)

SAMPLED_DAEMON_HOST = os.getenv("SAMPLED_DAEMON_HOST", "localhost")
SAMPLED_DAEMON_PORT = os.getenv("SAMPLED_DAEMON_PORT", 7675)
SAMPLED_MESSAGE_SEPARATOR = "\x00"

FieldType = Union[str, float, int, List[str]]

def _now_milliseconds():
    return int(time.time() * 1000)


class SampleBuffer:
    __buffer = []
    __last_flush = _now_milliseconds()

    def __init__(self):
        _sampled_thread = threading.Thread(
            target=self._flush_loop,
            name="SampledPublisher",
            daemon=True
        )
        _sampled_thread.start()
        
    def _flush_loop(self):
        while True:
            time.sleep(0.05)
            now = _now_milliseconds()
            buf_len = len(self.__buffer)
            if buf_len == 0:
                continue
            
            if buf_len >= SAMPLED_BUFFER_FLUSH_SIZE:
                self._do_flush()
                continue

            if now >= (self.__last_flush + SAMPLED_BUFFER_FLUSH_INTERVAL_MS):
                self._do_flush()

    def _do_flush(self):
        buffer, self.__buffer = self.__buffer, []
        self.__last_flush = _now_milliseconds()
        output = SAMPLED_MESSAGE_SEPARATOR.join(json.dumps(s) for s in buffer)

        attempt = 1
        sock = None
        while attempt <= 3:
            try:
                sock = socket.create_connection((SAMPLED_DAEMON_HOST, SAMPLED_DAEMON_PORT))
                sock.sendall(output.encode())
                sock.close()
                return
            except Exception as e:
                if sock is not None:
                    sock.close()
                print(f"Failed to push samples, attempt: {attempt}, error: {e}")
                attempt += 1
                time.sleep(1)

    def add_sample(self, dataset: str, sample: Dict[str, any]):
        self.__buffer.append({
            "timestamp": _now_milliseconds(),
            "dataset": dataset,
            "sample": sample
        })


sample_buffer = SampleBuffer()


def add_sample(dataset: str, sample: Dict[str, FieldType]):
    sample_buffer.add_sample(dataset, sample)
