import json
import importlib
import uuid
from pathlib import Path


class Config:
    """
    Stores FaaSr settings
    Batch logging
    """
    _config_file = None
    _log_buffer = None
    
    def __init__(self, config_path):
        if Config._config_file is None:
            Config._config_file = config_path

            # immutable state -- used to restore config
            # to what it was at the start of the function
            self._SKIP_SCHEMA_VALIDATE = self.SKIP_SCHEMA_VALIDATE
            self._SKIP_WF_VALIDATE = self.SKIP_WF_VALIDATE
            self._SKIP_REAL_TRIGGERS = self.SKIP_REAL_TRIGGERS
            self._SKIP_USER_FUNCTION = self.SKIP_USER_FUNCTION
            self._USE_LOCAL_USER_FUNC = self.USE_LOCAL_USER_FUNC
            self._LOCAL_FUNCTION_PATH = self.LOCAL_FUNCTION_PATH
            self._LOCAL_FUNCTION_NAME = self.LOCAL_FUNCTION_NAME
            self._LOCAL_FUNC_ARGS = self.LOCAL_FUNC_ARGS
            self._USE_LOCAL_FILE_SYSTEM = self.USE_LOCAL_FILE_SYSTEM
            self._LOCAL_FILE_SYSTEM_DIR = self.LOCAL_FILE_SYSTEM_DIR

            # initialize log
            Config._log_buffer = []
        else:
            raise RuntimeError("cannot initialize Config outside of debug_config.py")

    def __del__(self):
        """
        FLUSH SHOULD BE CALLED EXPLICITLY AT END OF EACH PROCESS -- destructor is a backup to flush log buffer
        """
        try:
            if Config._log_buffer:
                self.flush_log()
        except Exception as e:
            err_msg = f'{{debug_config.py: failed to flush log in destructor -- {e}}}'
            print(err_msg)

    def _read_config(self, key):
        """
        Read config entry
        """
        with open(Config._config_file, "r") as f:
            config = json.load(f)
        return config[key]

    def _write_config(self, key, value):
        """
        Write to config
        """
        with open(Config._config_file, "r+") as f:
            config = json.load(f)
            config[key] = value
            f.seek(0)
            json.dump(config, f, indent=4)
            f.truncate()

    def restore(self):
        """
        Reset configs to their original settings
        """
        self.SKIP_SCHEMA_VALIDATE = self.__dict__["_SKIP_SCHEMA_VALIDATE"]
        self.SKIP_WF_VALIDATE = self.__dict__["_SKIP_WF_VALIDATE"]
        self.SKIP_REAL_TRIGGERS = self.__dict__["_SKIP_REAL_TRIGGERS"]
        self.SKIP_USER_FUNCTION = self.__dict__["_SKIP_USER_FUNCTION"]
        self.USE_LOCAL_USER_FUNC = self.__dict__["_USE_LOCAL_USER_FUNC"]
        self.LOCAL_FUNCTION_PATH = self.__dict__["_LOCAL_FUNCTION_PATH"]
        self.LOCAL_FUNC_ARGS = self.__dict__["_LOCAL_FUNC_ARGS"]
        self.USE_LOCAL_FILE_SYSTEM = self.__dict__["_USE_LOCAL_FILE_SYSTEM"]
        self.LOCAL_FILE_SYSTEM_DIR = self.__dict__["_LOCAL_FILE_SYSTEM_DIR"]

    def log(self, message):
        """
        Stores logs to be batch uploaded to S3 at end of function
        """
        Config._log_buffer.append(message)

    def flush_log(self):
        """
        (to-do) Uploads all messages inside of config to DevLog and clear buffer
        """
        print('config log upload not implemented')

    """
    Getter and setter methods do not update internal member variables.
    Rather, they read to and write to the config.json file specified
    by Config._config_file, ensuring that state remains coherent
    between processes using the config
    """
    @property
    def SKIP_SCHEMA_VALIDATE(self):
        return self._read_config("SKIP_SCHEMA_VALIDATE")

    @SKIP_SCHEMA_VALIDATE.setter
    def SKIP_SCHEMA_VALIDATE(self, value):
        if not isinstance(value, bool):
            raise TypeError("SKIP_SCHEMA_VALIDATE must be a boolean")
        self._write_config("SKIP_SCHEMA_VALIDATE", value)

    @property
    def SKIP_WF_VALIDATE(self):
        return self._read_config("SKIP_WF_VALIDATE")

    @SKIP_WF_VALIDATE.setter
    def SKIP_WF_VALIDATE(self, value):
        if not isinstance(value, bool):
            raise TypeError("SKIP_WF_VALIDATE must be a boolean")
        self._write_config("SKIP_WF_VALIDATE", value)

    @property
    def SKIP_REAL_TRIGGERS(self):
        return self._read_config("SKIP_REAL_TRIGGERS")

    @SKIP_REAL_TRIGGERS.setter
    def SKIP_REAL_TRIGGERS(self, value):
        if not isinstance(value, bool):
            raise TypeError("SKIP_REAL_TRIGGERS must be a boolean")
        self._write_config("SKIP_REAL_TRIGGERS", value)

    @property
    def SKIP_USER_FUNCTION(self):
        return self._read_config("SKIP_USER_FUNCTION")

    @SKIP_USER_FUNCTION.setter
    def SKIP_USER_FUNCTION(self, value):
        if not isinstance(value, bool):
            raise TypeError("SKIP_USER_FUNCTION must be a boolean")
        self._write_config("SKIP_USER_FUNCTION", value)

    @property
    def USE_LOCAL_USER_FUNC(self):
        return self._read_config("USE_LOCAL_USER_FUNC")

    @USE_LOCAL_USER_FUNC.setter
    def USE_LOCAL_USER_FUNC(self, value):
        if not isinstance(value, bool):
            raise TypeError("USE_LOCAL_USER_FUNC must be a boolean")
        self._write_config("USE_LOCAL_USER_FUNC", value)

    @property
    def LOCAL_FUNCTION_PATH(self):
        return self._read_config("LOCAL_FUNCTION_PATH")

    @LOCAL_FUNCTION_PATH.setter
    def LOCAL_FUNCTION_PATH(self, value):
        if not isinstance(value, str):
            raise TypeError("LOCAL_FUNCTION_PATH must be a string")
        self._write_config("LOCAL_FUNCTION_PATH", value)

    @property
    def LOCAL_FUNCTION_NAME(self):
        return self._read_config("LOCAL_FUNCTION_NAME")

    @LOCAL_FUNCTION_NAME.setter
    def LOCAL_FUNCTION_NAME(self, value):
        if not isinstance(value, str):
            raise TypeError("LOCAL_FUNCTION_NAME must be a string")
        self._write_config("LOCAL_FUNCTION_NAME", value)

    @property
    def LOCAL_FUNC_ARGS(self):
        return self._read_config("LOCAL_FUNC_ARGS")

    @LOCAL_FUNC_ARGS.setter
    def LOCAL_FUNC_ARGS(self, value):
        if not isinstance(value, (list, dict)):
            raise TypeError("LOCAL_FUNC_ARGS must be a list or dict")
        self._write_config("LOCAL_FUNC_ARGS", value)

    @property
    def USE_LOCAL_FILE_SYSTEM(self):
        return self._read_config("USE_LOCAL_FILE_SYSTEM")

    @USE_LOCAL_FILE_SYSTEM.setter
    def USE_LOCAL_FILE_SYSTEM(self, value):
        if not isinstance(value, bool):
            raise TypeError("USE_LOCAL_FILE_SYSTEM must be a boolean")
        self._write_config("USE_LOCAL_FILE_SYSTEM", value)

    @property
    def LOCAL_FILE_SYSTEM_DIR(self):
        return self._read_config("LOCAL_FILE_SYSTEM_DIR")

    @LOCAL_FILE_SYSTEM_DIR.setter
    def LOCAL_FILE_SYSTEM_DIR(self, value):
        if not isinstance(value, str):
            raise TypeError("LOCAL_FILE_SYSTEM_DIR must be a string")
        self._write_config("LOCAL_FILE_SYSTEM_DIR", value)


directory = Path(__file__).parent.absolute()
config_file = directory / "config.json"
global_config = Config(config_file)