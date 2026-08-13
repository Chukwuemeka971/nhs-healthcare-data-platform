import logging


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a configured logger.

    Args:
        name:
            Name of the module requesting the logger.

    Returns:
        Configured logger instance.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Prevent duplicate handlers when the logger
    # is requested multiple times.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger