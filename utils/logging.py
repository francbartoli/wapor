import daiquiri
import logging
import sys


class Log():

    def __init__(self, level):

        self._level = level

    def initialize(self):
        
        daiquiri.setup(
                level=self.level,
                outputs=(
                    daiquiri.output.File(
                        directory="/tmp/wapor",
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
        else:
            return 20