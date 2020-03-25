import os

from linetimer import CodeTimer
from load_data import load
from download_data import download
from metadata_loader import run_metadata_load

if __name__ == "__main__":
    with CodeTimer("Downloader", unit="s"):
        if not os.environ["ENV"] == "DEVELOPMENT":
            download()
    with CodeTimer("Importer", unit="s"):
        load()

    with CodeTimer("Import Metadata.csv", unit="s"):
        run_metadata_load()
