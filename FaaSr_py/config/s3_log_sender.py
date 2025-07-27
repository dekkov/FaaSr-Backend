import logging
import sys

from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


class S3LogSender:
    """
    Uploads dev logs to S3
    """
    _sender = None

    def __new__(cls):
        """
        Singleton pattern to ensure only one instance of S3LogSender exists
        """
        if cls._sender is None:
            cls._sender = super(S3LogSender, cls).__new__(cls)
            cls._sender._initialized = False 
        return cls._sender

    def __init__(self):
        if self._initialized:
            return
        S3LogSender._sender = self
        self._initialized = True
        self._log_buffer = []
        self._start_time = datetime.now()
        self._faasr_payload = None 

    @property
    def faasr_payload(self):
        """
        Returns the faasr_payload for the logger
        """
        return self._faasr_payload

    @faasr_payload.setter
    def faasr_payload(self, faasr_payload):
        """  
        Sets the faasr_payload for the logger
        """
        self._faasr_payload = faasr_payload

    def log(self, message):
        """
        Adds a message to the log buffer

        Arguments:
            message: str -- message to log
        """
        if not message:
            raise RuntimeError("Cannot log empty message")
        self._log_buffer.append(message)

    def flush_log(self):
        """
        Uploads all messages inside S3LogSender and clears buffer
        """
        if not self._faasr_payload:
            logger.error("S3LogSender payload is not set")
            sys.exit(1)
        if not self._log_buffer:
            return
        
        logger.debug(f"Flushing S3 logs")

        # Combine all log messages into a single string and clear buffer
        full_log = "\n".join(self._log_buffer)
        self._log_buffer = []

        # Lazily import faasr_log to avoid circular imports
        from FaaSr_py.s3_api.log import faasr_log

        # Upload the log to S3
        faasr_log(self._faasr_payload, full_log)

    def get_curr_timestamp(self):
        """
        Returns the current timestamp in seconds since the start of the function
        """
        elapsed_time = datetime.now() - self._start_time
        seconds = round(elapsed_time.total_seconds(), 3)
        return seconds

s3_sender = S3LogSender() # Singleton instance