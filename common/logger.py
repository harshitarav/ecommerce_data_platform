import json
import logging
from datetime import datetime


class ETLLogger:

    def __init__(self, job_name, layer, table_name):

        self.job_name = job_name
        self.layer = layer
        self.table_name = table_name

        self.logger = logging.getLogger(job_name)

        if not self.logger.handlers:

            self.logger.setLevel(logging.INFO)

            handler = logging.StreamHandler()

            formatter = logging.Formatter("%(message)s")

            handler.setFormatter(formatter)

            self.logger.addHandler(handler)

    def _log(self, level, event, message, **kwargs):

        log_record = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": level,
            "job_name": self.job_name,
            "layer": self.layer,
            "table_name": self.table_name,
            "event": event,
            "message": message
        }

        log_record.update(kwargs)

        log_message = json.dumps(log_record)

        log_methods = {
            "INFO": self.logger.info,
            "WARNING": self.logger.warning,
            "ERROR": self.logger.error
        }

        log_methods.get(level, self.logger.info)(log_message)

    def info(self, event, message, **kwargs):

        self._log(
            level="INFO",
            event=event,
            message=message,
            **kwargs
        )

    def warning(self, event, message, **kwargs):

        self._log(
            level="WARNING",
            event=event,
            message=message,
            **kwargs
        )

    def error(self, event, message, **kwargs):

        self._log(
            level="ERROR",
            event=event,
            message=message,
            **kwargs
        )

    def metric(
        self,
        rows_read,
        rows_written,
        rows_rejected,
        execution_time,
        status
    ):

        self._log(
            level="INFO",
            event="ETL_METRICS",
            message="ETL Execution Metrics",
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            execution_time_seconds=execution_time,
            status=status
        )