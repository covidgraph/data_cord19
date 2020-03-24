import os
from Configs import ConfigBase

# define different classes per environment


class DEFAULT(ConfigBase):
    # commit every n files
    COMMIT_INTERVAL = 100

    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )

    DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "dataset/")
    DATA_DIRS = [
        "biorxiv_medrxiv",
        "comm_use_subset",
        "noncomm_use_subset",
        "custom_license",
    ]
    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")

    # Override label names
    JSON2GRAPH_LABELOVERRIDE = {
        "authors": "Author",
    }
    JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME = "_hash_id"
    # Define for which labels and how a hash id attr should be generated
    JSON2GRAPH_GENERATED_HASH_IDS = {
        "Abstract": ["text"],  # Generate an id based on the property "text"
        "Affiliation": "AllAttributes",  # Generate an id based all properties
        "Author": "AllAttributes",
        "Back_matter": "AllAttributes",
        "Bibref": "AllAttributes",
        "Bib_entries": "AllInnerContent",  # Generate an id based all attr and childrens attr
        "Body_text": "AllAttributes",
        "Cite_spans": "AllInnerContent",
        "Figref": "AllAttributes",
        "Location": "AllAttributes",
        "Metadata": "AllInnerContent",
        "Other_ids": "AllInnerContent",
        "Ref_entries": "AllInnerContent",
        "Ref_spans": "AllInnerContent",
        "Tabref": "AllAttributes",
    }

    # Define which properties can be taken as primary key for specific labels
    # {"label":"attribute-that-works-as-id"}
    JSON2GRAPH_ID_ATTR = {
        "Arxiv": "arXiv",
        "Doi": "DOI",
        "Paper": "paper_id",
        "Pmcid": "PMCID",
        "Pmid": "PMID",
    }
    JSON2GRAPH_CONCAT_LIST_ATTR = {"middle": " "}
    JSON2GRAPH_COLLECTION_NODE_LABEL = "CollectionHub"


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
