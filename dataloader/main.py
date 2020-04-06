import os
import sys
from linetimer import CodeTimer

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )
    SCRIPT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(SCRIPT_DIR))


from dataloader.download_data import download
from dataloader.load_data import load_data

if __name__ == "__main__":
    with CodeTimer("Downloader", unit="s"):
        download()
    with CodeTimer("Importer", unit="s"):
        load_data()
