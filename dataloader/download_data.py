import os
import logging

import cotools
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def download():

    log.info("Start downloading CORD-19 Dataset...")
    if not os.path.isdir(config.DATA_BASE_DIR):
        os.makedirs(config.DATA_BASE_DIR)
    cotools.download(dir=config.DATA_BASE_DIR)
    log.info("Finished downloading CORD-19 Dataset...")


if __name__ == "__main__":
    download()
