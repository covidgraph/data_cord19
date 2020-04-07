import os
import multiprocessing
from Configs import ConfigBase

# define different classes per environment


class DEFAULT(ConfigBase):
    # commit every n nodes/relations
    COMMIT_INTERVAL = 10000
    # Bundle workloads to <PAPER_BATCH_SIZE>-papers and load them into database
    PAPER_BATCH_SIZE = 500
    NO_OF_PROCESSES = multiprocessing.cpu_count() - 1 or 1
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )

    DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "dataset/")

    # {CORRECT_FORMAT:[OCCURENT_FORMAT]}
    PAPER_ID_NAME_NORMALISATION = {
        "DOI": ["Doi", "doi"],
        "arXiv": ["arxiv", "ARXIV"],
        "Pmcid": ["pmcid", "PMICD"],
    }

    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")

    # Column names will be take over in the created nodes attributes or child nodes.
    # if you are not happy with the names you can overide them here.
    # follow the format from
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html
    # {"old_name":"new_name", "other_column_old_name", "other_column_new_name"}
    METADATA_FILE_COLUMN_OVERRIDE = {
        "WHO #Covidence": "who_covidence",
        "authors": "author",
        "Microsoft Academic Paper ID": "microsoft_academic_id",
        "source_x": "source",
    }
    METADATA_FILE_ID_COLUMNS = [
        "doi",
        "pmcid",
        "pubmed_id",
        "microsoft_academic_id",
        "who_covidence",
    ]
    METADATA_PAPER_PROPERTY_COLUMNS = [
        "source",
        "title",
        "publish_time",
        "journal",
        "url",
    ]

    FULLTEXT_PAPER_BIBREF_ATTRS = [
        "ref_id",
        "title",
        "year",
        "venue",
        "volume",
        "issn",
        "pages",
    ]

    JSON2GRAPH_LABEL_OVERRIDE = {
        "location": "Location",
        "cite_spans": "Citation",
        "affiliation": "Affiliation",
    }

    JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME = "_hash_id"
    # Define for which labels and how a hash id attr should be generated
    JSON2GRAPH_GENERATED_HASH_IDS = {
        "BodyText": "AllAttributes",
        "Paper": "AllAttributes",
        "Reference": "AllInnerContent",
        "Location": "AllAttributes",
        "Abstract": ["text"],  # Generate an id based on the property "text"
        "Affiliation": "AllAttributes",  # Generate an id based all properties
        "Author": "AllAttributes",
        "Back_matter": "AllAttributes",
        "Bibref": "AllAttributes",
        "Bib_entries": "AllInnerContent",  # Generate an id based all attr and childrens attr
        "Cite_spans": "AllInnerContent",
        "Figref": "AllAttributes",
        "Metadata": "AllInnerContent",
    }
    JSON2GRAPH_CONCAT_LIST_ATTR = {"middle": " "}
    JSON2GRAPH_COLLECTION_NODE_LABEL = "{LIST_MEMBER_LABEL}Collection"
    JSON2GRAPH_COLLECTION_EXTRA_LABELS = []


# All following config classes inherit from DEFAULT
class PRODUCTION(DEFAULT):
    pass


class DEVELOPMENT(DEFAULT):
    COMMIT_INTERVAL = 2
    DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    DATA_DIRS = [
        "test",
    ]
    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")
