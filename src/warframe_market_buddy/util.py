import sys
import logging

def setup_logging(root_level=logging.DEBUG):
    # Create a formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create handlers for stdout and stderr
    stdout_handler = logging.StreamHandler(sys.stdout)

    # Set the log level for each handler
    stdout_handler.setLevel(logging.DEBUG)

    # Set the formatter for each handler
    stdout_handler.setFormatter(formatter)

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)

    async_logger = logging.getLogger("asyncio")
    async_logger.setLevel(logging.INFO)

    # Add the handlers to the root logger
    root_logger.addHandler(stdout_handler)
