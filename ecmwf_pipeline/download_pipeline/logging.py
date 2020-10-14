import io
import logging


class LoggerIO(io.StringIO):
    """Wrapper to treat `logging.Logger` as file-like object."""

    def __init__(self, logger: logging.Logger, log_level: int = logging.INFO):
        self.lgr = logger
        self.lvl = log_level
        self.buf = ''

    def __del__(self):
        super(LoggerIO, self).__del__()
        self.flush()

    def write(self, buf: str) -> None:
        tmp_buf, self.buf = self.buf + buf, ''

        for line in tmp_buf.splitlines(keepends=True):
            if line.endswith('\n'):
                self.lgr.log(self.lvl, line.rstrip())
            else:
                self.buf += line

    def flush(self) -> None:
        if self.buf:
            self.lgr.log(self.lvl, self.buf.rstrip())
        self.buf = ''
