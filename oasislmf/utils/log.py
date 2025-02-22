__all__ = [
    'oasis_log',
    'read_log_config',
    'set_rotating_logger'
]

"""
Logging utils.
"""
import inspect
import logging
import os
import time

from functools import wraps
from logging.handlers import RotatingFileHandler


def getargspec(func):
    if hasattr(inspect, 'getfullargspec'):
        return inspect.getfullargspec(func)
    else:
        return inspect.getargspec(func)


def set_rotating_logger(
    log_file_path=inspect.stack()[1][1],
    log_level=logging.INFO,
    max_file_size=10**7,
    max_backups=5
):
    _log_fp = log_file_path
    if not os.path.isabs(_log_fp):
        _log_fp = os.path.abspath(_log_fp)

    log_dir = os.path.dirname(_log_fp)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    handler = RotatingFileHandler(
        _log_fp,
        maxBytes=max_file_size,
        backupCount=max_backups
    )

    logging.getLogger().setLevel(log_level)
    logging.getLogger().addHandler(handler)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s")

    handler.setFormatter(formatter)


def read_log_config(config_parser):
    """
    Read an Oasis standard logging config
    """
    log_file = config_parser['LOG_FILE']
    log_level = config_parser['LOG_LEVEL']
    log_max_size_in_bytes = int(config_parser['LOG_MAX_SIZE_IN_BYTES'])
    log_backup_count = int(config_parser['LOG_BACKUP_COUNT'])

    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    handler = RotatingFileHandler(
        log_file, maxBytes=log_max_size_in_bytes,
        backupCount=log_backup_count)
    logging.getLogger().setLevel(log_level)
    logging.getLogger().addHandler(handler)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)


def oasis_log(*args, **kwargs):
    """
    Decorator that logs the entry, exit and execution time.
    """
    logger = logging.getLogger()

    def actual_oasis_log(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            caller_module_name = func.__globals__.get('__name__')
            logger.info("STARTED: {}.{}".format(
                caller_module_name, func_name))

            args_name = getargspec(func)[0]
            args_dict = dict(zip(args_name, args))

            for key, value in args_dict.items():
                if key == "self":
                    continue
                logger.debug("    {} == {}".format(key, value))

            if len(args) > len(args_name):
                for i in range(len(args_name), len(args)):
                    logger.debug("    {}".format(args[i]))
            for key, value in kwargs.items():
                logger.debug("    {} == {}".format(key, value))

            # Get the time at the start
            start = time.time()

            # Run the function
            result = func(*args, **kwargs)

            # Get the time at the end
            end = time.time()

            # Get the duration in mins and seconds
            mins, secs = divmod(end - start, 60)

            # Print the logger message
            logger.info(
                "COMPLETED: {}.{} in {:02d}m {:05.2f}s".format(
                    caller_module_name, func_name, round(mins), round(secs, 2)))

            # Return the result
            return result
        return wrapper

    if len(args) == 1 and callable(args[0]):
        return actual_oasis_log(args[0])
    else:
        return actual_oasis_log
