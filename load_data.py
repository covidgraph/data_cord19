import os
import json
import pathlib
import multiprocessing
import numpy as np
from py2neo.database import ClientError
import time
import random

# from graphio import NodeSet, RelationshipSet
from py2neo import Graph, Schema
from DZDjson2GraphIO import Json2graphio

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )


# GRAPH = Graph("bolt://192.168.178.77:7687")
GRAPH = Graph()

# commit every n files
COMMIT_INTERVAL = 100


DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "dataset/2020-03-13/")
DATA_DIRS = [
    "biorxiv_medrxiv/biorxiv_medrxiv",
    "comm_use_subset/comm_use_subset",
    "noncomm_use_subset/noncomm_use_subset",
    "pmc_custom_license/pmc_custom_license",
]

"""
DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "testdataset")
DATA_DIRS = [
    "test",
]
"""


# Override label names
JSON2GRAPH_LABELOVERRIDE = {
    "authors": "Author",
}

# Define for which labels and how a hash id attr should be generated
JSON2GRAPH_GENERATED_IDS = {
    "Abstract": ["text"],  # Generate an id based on the property "text"
    "Affiliation": "AllAttributes",  # Generate an id based all properties
    "Author": "AllAttributes",
    "Back_matter": "AllAttributes",
    "Bibref": "AllAttributes",
    "Bib_entries": "AllInnerContent",  # Generate an id based all attr and childrens attr
    "Body_text": "AllAttributes",
    "Cite_spans": "AllAttributes",
    "Figref": "AllAttributes",
    "Location": "AllAttributes",
    "Metadata": "AllAttributes",
    "Other_ids": "AllInnerContent",
    "Ref_entries": "AllInnerContent",
    "Ref_spans": "AllAttributes",
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
JSON2GRAPH_GENERATED_ID_ATTR_NAME = "_hash_id"


class DataLoader(object):
    def __init__(self, jsons_file_list: list):

        self.files = jsons_file_list

        c = Json2graphio()
        c.config_dict_label_override = JSON2GRAPH_LABELOVERRIDE
        c.config_func_custom_relation_name_generator = DataTransformer.nameRelation
        c.config_dict_primarykey_generated_hashed_attrs_by_label = (
            JSON2GRAPH_GENERATED_IDS
        )
        c.config_dict_concat_list_attr = JSON2GRAPH_CONCAT_LIST_ATTR
        c.config_dict_primarykey_attr_by_label = JSON2GRAPH_ID_ATTR
        c.config_str_primarykey_generated_attr_name = JSON2GRAPH_GENERATED_ID_ATTR_NAME
        c.config_func_node_pre_modifier = DataTransformer.renameLabels
        self.loader = c

    def load_json(self):
        for file in self.files:
            json_data = None
            with open(file) as json_file:
                json_data = json.load(json_file)
            self.loader.load_json("Paper", json_data)
        print("LOAD JSON FINISH")

    def merge(self, graph: Graph):
        print("START Loding into db")
        self.loader.create_indexes(graph)
        self.loader.merge(graph)


class DataTransformer(object):
    @classmethod
    def renameLabels(cls, node):
        for label in node.labels:
            if label.startswith("Bibref"):
                node.remove_label(label)
                node.add_label("Bibref")
                node.__primarylabel__ = "Bibref"
            if label.startswith("Figref"):
                node["_id"] = label
                node.remove_label(label)
                node.add_label("Figref")
                node.__primarylabel__ = "Figref"
            if label.startswith("Tabref"):
                node["_id"] = label
                node.remove_label(label)
                node.add_label("Tabref")
                node.__primarylabel__ = "Tabref"
        return node

    @classmethod
    def nameRelation(cls, parent_node, child_node, relation_props):
        names = []
        for node in [parent_node, child_node]:

            if node.__primarylabel__.startswith("Bibref"):
                names.append("BIBREF")
            elif node.__primarylabel__.startswith("Figref"):
                names.append("FIGREF")
            elif node.__primarylabel__.startswith("Tabref"):
                names.append("TABREF")
            else:
                names.append(node.__primarylabel__.upper())
        return "{}_HAS_{}".format(names[0], names[1])


class GraphSchema(object):
    @classmethod
    def create_uniqueness_constraint(cls):
        for label in JSON2GRAPH_GENERATED_IDS.keys():
            cls._create_constraint(label, JSON2GRAPH_GENERATED_ID_ATTR_NAME)
        for label, attr in JSON2GRAPH_ID_ATTR.items():

            cls._create_constraint(label, attr)

    @classmethod
    def _create_constraint(cls, label, attr):
        try:
            tx = GRAPH.begin()
            cypher = "CREATE CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
                label, attr
            )
            print(cypher)
            tx.run(cypher)
            tx.commit()
        except ClientError:
            pass


class Worker(multiprocessing.Process):
    def __init__(self, worker_name: str, json_files: list):
        super(Worker, self).__init__()
        self.name = worker_name
        self.files = json_files

    def run(self):
        print(self.name, " STARTING!")
        files_cnt_total = len(self.files)
        files_cnt = 1
        for file in self.files:
            print(
                self.name,
                "Start loading {} of {} files.".format(files_cnt, files_cnt_total),
            )
            files_cnt += 1
            loader = DataLoader(file)

            loader.load_data()
        print(self.name, " FINISHED!")

    @classmethod
    def generate_workers(cls, file_directory: str):
        # files = filter(lambda x: os.path.isfile(x), os.listdir(file_directory))
        files = [
            filepath.absolute()
            for filepath in pathlib.Path(file_directory).glob("**/*")
        ]
        file_buckets = [list(ar) for ar in np.array_split(files, WORKER_COUNT)]

        workers = []
        worker_no = 1
        for bucket in file_buckets:
            workers.append(cls("WORKER_{}".format(worker_no), bucket))
            worker_no += 1
        return workers


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def start():
    # print("Create contraints/indexes")
    # GraphSchema.create_uniqueness_constraint()
    for index, datadir in enumerate(DATA_DIRS):
        dir_ = os.path.join(DATA_BASE_DIR, datadir)

        files = [filepath.absolute() for filepath in pathlib.Path(dir_).glob("**/*")]
        print("STARTING WITH DIR {}.".format(dir_))

        file_buckets = list(chunks(files, COMMIT_INTERVAL))
        print(
            "Seperating {} files in {} buckets.".format(len(files), len(file_buckets))
        )
        for bucket_index, file_bucket in enumerate(file_buckets):
            print(
                "DIR {} of {}".format(index, len(DATA_DIRS)),
                "FILE-BUCKET {} of {}".format(bucket_index, len(file_buckets)),
            )
            loader = DataLoader(file_bucket)
            loader.load_json()
            loader.merge(GRAPH)


GraphSchema.create_uniqueness_constraint()
start()
