import logging
import platform
import sys


logger = logging.getLogger("streamforge.monitoring")


def get_system_status():
    try:
        status = {
            "status": "healthy",
            "service": "streamforge",
            "python_version": sys.version.split()[0],
            "platform": platform.system(),
        }

        logger.info("System status check successful")
        return status

    except Exception as error:
        logger.error("System status check failed: %s", error)
        return {
            "status": "unhealthy",
            "service": "streamforge",
            "error": str(error),
        }