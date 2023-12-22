"""Common module has shared resources and functionalities """
from collections import deque
import logging

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.WARNING)

class FhirQueue(object):
    """A singleton class used in multiple modules"""
    _common_instance = None
    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._common_instance, cls):
            cls._common_instance = object.__new__(cls, *args, **kwargs)
        return cls._common_instance

    def __init__(self, max_queue_items = 100000):
        self._queue = deque(maxlen=max_queue_items)

    def enqueue(self, item):
        """push item to queue. Risk of overflow"""
        self._queue.append(item)

    def dequeue(self):
        """return item from the end of the queue FIFO"""
        ret_val = None
        try:
            ret_val = self._queue.pop()
        except ValueError as ex:
            logging.error("Error in dequeue: %s", str(ex))
        return ret_val

