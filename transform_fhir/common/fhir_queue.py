"""Common module has shared resources and functionalities """
from collections import deque
import logging

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.WARNING)

class FhirQueue(object):
    """A singleton class used in multiple modules"""
    _common_instance = None
    _queue = None
    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._common_instance, cls):
            cls._common_instance = object.__new__(cls, *args, **kwargs)
            cls._queue = deque(maxlen=10000)
        return cls._common_instance

    def enqueue(self, item):
        """push item to queue. Risk of overflow"""
        self._queue.append(item)
        print(len(self._queue))

    def dequeue(self):
        """return item from the end of the queue FIFO"""
        ret_val = None
        try:
            ret_val = self._queue.pop()
        except ValueError as ex:
            logging.error("Error in dequeue: %s", str(ex))
        return ret_val
