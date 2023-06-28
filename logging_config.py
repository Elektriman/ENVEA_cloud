import logging

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create handlers
debug_handler = logging.FileHandler('logs/debug.log')
debug_handler.setLevel(logging.DEBUG)
info_handler = logging.FileHandler('logs/info.log')
info_handler.setLevel(logging.INFO)

# Create formatters
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
info_formatter = logging.Formatter('%(asctime)s - %(message)s')

# Add formatters to handlers
debug_handler.setFormatter(debug_formatter)
info_handler.setFormatter(info_formatter)

# Add handlers to logger
logger.addHandler(debug_handler)
logger.addHandler(info_handler)

def log_errors(f):
    """
    intercepts the error output to redirect it in a log file via a wrapped function

    :param f: The function to be decorated
    :return: A function that wraps the function f.
    """
    def inner(*args, **kwargs):
        try:
            res = f(*args, **kwargs)
        except Exception as e:
            logger.error(f"The following exception occured while executing {f.__module__}.{f.__qualname__}")
            logger.error(f'{e.__class__.__name__} : {e}')
        else :
            return res

    return inner