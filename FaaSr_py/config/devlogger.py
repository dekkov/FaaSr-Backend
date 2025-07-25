import logging

from datetime import datetime, timedelta
from FaaSr_py.s3_api.log import faasr_log


logger = logging.getLogger(__name__)


#to-do: make DevLogger the handler for error, performance, and warning logging
class DevLogger:
    # DevLogger class for managing debug logs in FaaSr
    _DevLogger = None
    def __init__(self):
        if not DevLogger._DevLogger:
            DevLogger._DevLogger = self
            self._log_buffer = []
            self._start_time = datetime.now()
            self._payload = None 
            self._log_file = None
        else:
            raise RuntimeError("DevLogger cannot be initialized more than once")
        
    @property
    def payload(self):
        """
        Returns the FaaSr payload for the logger
        """
        return self._payload
        
    @payload.setter
    def payload(self, faasr_payload):
        """  
        Sets the FaaSr payload for the logger
        """
        self._payload = faasr_payload
        self._log_file = self._payload['FunctionInvoke'] + ".txt"
    
    def log(self, message):
        """
        Adds a message to the log buffer

        Arguments:
            message: str -- message to log
        """
        if not message:
            print("{debug_config.py: ERROR -- log message is empty}")
            return
        self._log_buffer.append(message)

    def flush_log(self):
        """
        Uploads all messages inside of config to DevLog and clear buffer

        Arguments:
            config: FaaSr payload dict -- used to get the log name
        """
        if not self._payload:
            print("{debug_config.py: ERROR -- no payload set for DevLogger}")
            raise RuntimeError("DevLogger payload is not set")
        if not self._log_buffer:
            print("{debug_config.py: no logs to upload}")
            return
        # Combine all log messages into a single string and clear buffer
        full_log = "\n".join(self._log_buffer)
        self._log_buffer = []

        # Upload log
        faasr_log(config=self._payload, log_message=full_log, log_name="DevLog", file_name=self._log_file)

    def log_time(self, message):
        """
        Logs a message with the current time

        Arguments:
            message: str -- message to log
        """
        elapsed_time = datetime.now() - self._start_time
        seconds = round(elapsed_time.total_seconds(), 3)
        self.log(f"{seconds} - {message}")

    def __del__(self):
        """
        Destructor is a backup to flush log buffer, but should not be relied upon
        """
        try:
            if self._log_buffer:
                self.flush_log()
        except Exception as e:
            err_msg = f'{{debug_config.py: failed to flush log in destructor -- {e}}}'
            print(err_msg)

