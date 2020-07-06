import os
import py2neo
import multiprocessing
from Configs import ConfigBase

# define different classes per environment


class DEFAULT(ConfigBase):
    LOG_LEVEL = "INFO"
    NEO4J_CON = "bolt://localhost:7687"
    # commit every n nodes/relations
    COMMIT_INTERVAL = 10000
    # Bundle workloads to <PAPER_BATCH_SIZE>-papers and load them into database
    # Decrease this number if you RAM is limited on the loading system
    PAPER_BATCH_SIZE = 300
    # The number of simultaneously working parsing processes
    # Atm more than 3 or 4 makes no sense as there will build up a task queue for database loading
    NO_OF_PROCESSES = (
        4
        if multiprocessing.cpu_count() >= 4
        else (multiprocessing.cpu_count() - 1 or 1)
    )
    # if one worker fails should we cancel the whole import, or import the rest of the data.
    # you will get feedback on which rows the import failed
    CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS = True
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )

    # if set to True, the dataset will always be downloaded, regardless of its allready existing
    REDOWNLOAD_DATASET_IF_EXISTENT = False

    # Where to store the downloaded dataset
    DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "../dataset/")

    # Where the 'metadata.csv' file from the CORD-19 Dataset is stored
    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")

    # Paper IDs like 'DOI', Pmcid are coming in different spellings and cases
    # With this option you cann compensate for that
    # Format: {CORRECT_FORMAT:[OCCURENT_FORMAT]}
    PAPER_ID_NAME_NORMALISATION = {
        "DOI": ["Doi", "doi"],
        "arXiv": ["arxiv", "ARXIV"],
        "Pmcid": ["pmcid", "PMICD"],
    }

    # Column names, in the 'metadata.csv' file, will be taken over in the created nodes attributes or child nodes.
    # if you are not happy with the names you can overide them here.
    # follow the format from
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html
    # {"old_name":"new_name", "other_column_old_name", "other_column_new_name"}
    METADATA_FILE_COLUMN_OVERRIDE = {
        "who_covidence_id": "who_covidence",
        "authors": "author",
        "mag_id": "microsoft_academic_id",
        "source_x": "source",
    }

    # Define which columns in 'metadata.csv' are identifiers of a paper
    # They will appear as :PaperID nodes in the resulting graph
    METADATA_FILE_ID_COLUMNS = [
        "doi",
        "pmcid",
        "pubmed_id",
        "microsoft_academic_id",
        "who_covidence",
    ]

    # Define which columns in 'metadata.csv' are properties of a paper
    # they will appear as properties of the :Paper nodes
    METADATA_PAPER_PROPERTY_COLUMNS = [
        "cord_uid",
        "source",
        "title",
        "publish_time",
        "journal",
        "url",
    ]

    # in the full text json files in the CORD19 dataset, every paper has references located under the key "bib_entries".
    # Define here which attributes of these json objects you want to transfer to node properties to :Reference Nodes
    FULLTEXT_PAPER_BIBREF_ATTRS = [
        "ref_id",
        "title",
        "year",
        "venue",
        "volume",
        "issn",
        "pages",
    ]

    def get_graph(self):
        if "GC_NEO4J_URL" in os.environ:
            url = os.getenv("GC_NEO4J_URL")
            if "GC_NEO4J_USER" in os.environ and "GC_NEO4J_PASSWORD" in os.environ:
                user = os.getenv("GC_NEO4J_USER")
                pw = os.getenv("GC_NEO4J_PASSWORD")
                print("URL", url)
                print("pw", pw)
                print("user", user)
                return py2neo.Graph(url, password=pw, user=user)
            return py2neo.Graph(url)
        else:
            return py2neo.Graph(self.NEO4J_CON)


# All following config classes inherit from DEFAULT
class PRODUCTION(DEFAULT):
    pass


class SMOKETEST(DEFAULT):
    DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")


class DEVELOPMENT(DEFAULT):
    # DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    # METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")
    pass


class PROFILING(DEFAULT):
    # DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    # METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")
    NO_OF_PROCESSES = 1
    COMMIT_INTERVAL = 10000
    LOG_LEVEL = "DEBUG"
