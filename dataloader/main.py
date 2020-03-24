import os

from linetimer import CodeTimer
from load_data import load
from download_data import download

if __name__ == "__main__":
    with CodeTimer("Downloader", unit="s"):
        if not os.environ["ENV"] == "DEVELOPMENT":
            download()
    with CodeTimer("Importer", unit="s"):
        load()
