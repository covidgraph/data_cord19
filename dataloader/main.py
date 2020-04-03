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
from dataloader.load_abstracts import load_abstracts
from dataloader.load_fulltext_papers import load_fulltext_papers

if __name__ == "__main__":
    with CodeTimer("Downloader", unit="s"):
        download()
    with CodeTimer("Importer", unit="s"):
        load_fulltext_papers()
    with CodeTimer("Import Metadata.csv", unit="s"):
        load_abstracts()
