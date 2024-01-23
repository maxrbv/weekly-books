import logging
import threading


class ColorLogger(logging.Logger):
    def __init__(self, name, session_id, phone=None):
        self.session_id = session_id
        self.phone = phone
        super().__init__(name)
        self.lock = threading.Lock()

        self.colors = {
            'DEBUG': '\033[94m',  # Синий
            'INFO': '\033[92m',  # Зеленый
            'WARNING': '\033[93m',  # Желтый
            'ERROR': '\033[91m',  # Красный
            'CRITICAL': '\033[38;5;165m',  # Фиолетовый
            'RESOURCE': '\033[38;5;155m',  # Небесный (дополнительный AMI уровень)
            'DIALOG_TRACE': '\033[38;5;186m',  # Гамбожевый
        }
        self.reset_color = '\033[0m'
        logging.addLevelName(logging.INFO + 1, 'RESOURCE')
        logging.addLevelName(logging.INFO + 2, 'DIALOG_TRACE')
        if self.phone:
            fmt = f'{self.phone} | {self.session_id} | %(asctime)s | %(levelname)8s | %(message)s'
        else:
            fmt = f'{self.session_id} | %(asctime)s | %(levelname)8s | %(message)s'
        self.formatter = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S')

    def _get_color_code(self, levelname):
        return self.colors.get(levelname, '')

    def _format_msg(self, msg, levelname):
        color_code = self._get_color_code(levelname)
        reset_code = self.reset_color
        formatted_msg = f'{color_code}{msg}{reset_code}'
        return formatted_msg

    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
        with self.lock:
            if self.isEnabledFor(level):
                msg = self._format_msg(msg, logging.getLevelName(level))
                super()._log(level, msg, args, exc_info, extra, stack_info, stacklevel)

    def debug(self, msg, *args, **kwargs):
        self._log(logging.DEBUG, msg, args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log(logging.INFO, msg, args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log(logging.WARNING, msg, args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log(logging.ERROR, msg, args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._log(logging.CRITICAL, msg, args, **kwargs)

    def resource(self, msg, *args, **kwargs):
        self._log(logging.INFO + 1, msg, args, **kwargs)

    def data(self, msg, *args, **kwargs):
        self._log(logging.INFO + 2, msg, args, **kwargs)


def get_logger(name, session_id, phone=None):
    logger = ColorLogger(name, session_id, phone)
    handler = logging.StreamHandler()
    handler.setFormatter(logger.formatter)
    logger.addHandler(handler)
    return logger
