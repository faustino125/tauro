# Avoid ValueError: I/O operation on closed file produced by Loguru when threads continue
# writing after pytest streams are closed.
#
# Place this file at the repository root or in the tests directory so pytest loads it.
from loguru import logger


def pytest_configure(config):
    """
    Reconfigures loguru at the start of the test session:
    - Removes existing sinks (which may point to streams that pytest will close).
    - Adds a "no-op" sink that discards all messages (prevents thread exceptions).
    If you want to see logs during tests, replace the lambda with logger.add(sys.__stdout__).
    """
    try:
        logger.remove()
    except Exception:
        # ignore if no sinks or error removing
        pass

    # Sink that discards all messages (fast and safe for multi-threaded tests)
    logger.add(lambda msg: None, level="INFO", enqueue=True)
