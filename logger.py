import logging
import colorama

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

colorama.init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': colorama.Fore.CYAN,
        'INFO': colorama.Fore.GREEN,
        'WARNING': colorama.Fore.YELLOW,
        'ERROR': colorama.Fore.MAGENTA,
        'CRITICAL': colorama.Fore.MAGENTA,
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, colorama.Style.RESET_ALL)
        message = super().format(record)
        return f"{log_color}{message}{colorama.Style.RESET_ALL}"


colored_formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(colored_formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def error(message):
    logger.error(message)


def info(message):
    logger.info(message)


def debug(message):
    logger.debug(message)


def warning(message):
    logger.warning(message)
