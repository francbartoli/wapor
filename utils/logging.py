import daiquiri
import logging
import sys
import os


class Log():

    def __init__(self, level):

        self._level = level

    def initialize(self):
        log_dir = "/tmp/wapor"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        daiquiri.setup(
                level=self.level,
                outputs=(
                    daiquiri.output.File(
                        directory=log_dir,
                        level=self.level
                    ),
                    daiquiri.output.Stream(
                        formatter=daiquiri.formatter.ColorFormatter(
                            fmt=(daiquiri.formatter.DEFAULT_FORMAT)
                        )
                    )
                )
            )

    @property
    def level(self):
        if self._level in "DEBUG":
            return 10
        elif self._level in "INFO":
            return 20
        elif self._level in "WARNING":
            return 30
        elif self._level in "ERROR":
            return 40
        elif self._level in ["CRITICAL", "FATAL"]:
            return 50
        else:
            return 0
