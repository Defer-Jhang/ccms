import linecache
import logging
import os
import sys
import traceback
import logging
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler

class DebugOrErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno in (logging.DEBUG, logging.ERROR)


class DebugFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.DEBUG


class logging_api:
    """
    LogManager sets up a logger with both console and timed rotating file handlers.
    """

    def __init__(self, filename, level=logging.DEBUG, backup_count=7):
        try:
            self.logger = logging.getLogger()
            self.logger.setLevel(level)

            if not self.logger.handlers:
                log_formatter = logging.Formatter(
                    fmt="%(asctime)s %(levelname)-7s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )

                log_path = os.path.abspath(f"{filename}.log")
                os.makedirs(os.path.dirname(log_path), exist_ok=True)

                file_handler = ConcurrentTimedRotatingFileHandler(
                    filename=log_path,
                    when="midnight",
                    interval=1,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
                file_handler.setFormatter(log_formatter)

                console_handler = logging.StreamHandler()
                console_handler.setFormatter(log_formatter)

                self.logger.addHandler(file_handler)
                self.logger.addHandler(console_handler)

        except Exception:
            logging.error(
                "logger initialization failed\n%s",
                self.get_slim_error_log(),
            )

    def get_logger(self):
        """
        Return the configured logger.
        """
        return self.logger

    def get_slim_error_log(self, full=False):
        """
        Get a concise error log for the current exception.

        Args:
            full (bool): Return the complete traceback when True.

        Returns:
            str: Concise exception information.
        """
        try:
            exc_type, exc_value, exc_tb = sys.exc_info()

            if exc_type is None:
                return "No active exception."

            if full:
                return traceback.format_exc()

            # Move to the frame where the exception actually occurred.
            while exc_tb.tb_next is not None:
                exc_tb = exc_tb.tb_next

            frame = exc_tb.tb_frame
            filename = frame.f_code.co_filename
            function_name = frame.f_code.co_name
            line_number = exc_tb.tb_lineno

            source_code = linecache.getline(
                filename,
                line_number
            ).strip()

            return (
                f"Error: {exc_type.__name__}: {exc_value}\n"
                f"Location: {os.path.basename(filename)}:"
                f"{line_number} ({function_name})\n"
                f"Code: {source_code or 'Unavailable'}"
            )

        except Exception as log_error:
            # Never allow the log helper to crash the main system.
            try:
                logging.error(
                    "Failed to generate error log: %s",
                    log_error
                )
            except Exception:
                pass

            return "Error details unavailable."
