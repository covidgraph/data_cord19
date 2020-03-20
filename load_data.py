import os
import json
import pathlib
import multiprocessing
import numpy as np
from py2neo import Graph, Schema
from DZDjson2Graph import Json2graph

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )


GRAPH = Graph()
WORKER_COUNT = 4

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

# Define for which labels auto primary keys should be generated
JSON2GRAPH_GENERATED_IDS = {
    "Abstract": ["text"],  # Generate an id based on the property "text"
    "Affiliation": "All",  # Generate an id based all properties
    "Author": "All",
    "Back_matter": "All",
    "Bibref": "All",
    "Bib_entries": None,  # Generate a random id
    "Body_text": "All",
    "Cite_spans": "All",
    "Figref": "All",
    "Location": "All",
    "Metadata": "All",
    "Other_ids": None,
    "Ref_entries": None,
    "Ref_spans": "All",
    "Tabref": "All",
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


class DataLoader(object):
    def __init__(self, json_path):

        self.json_path = json_path

    def _parse_file(self):
        with open(self.json_path) as json_file:
            self.data = json.load(json_file)

    def _cast_json(self):
        c = Json2graph(self.data)
        c.config_dict_label_override = JSON2GRAPH_LABELOVERRIDE
        c.config_func_custom_relation_name_generator = DataTransformer.nameRelation
        c.config_dict_primarykey_generated_hashed_attrs_by_label = (
            JSON2GRAPH_GENERATED_IDS
        )
        c.config_dict_concat_list_attr = JSON2GRAPH_CONCAT_LIST_ATTR
        c.config_dict_primarykey_attr_by_label = JSON2GRAPH_ID_ATTR
        return c.get_subgraph("Paper")

    def load_data(self):
        self._parse_file()
        tx = GRAPH.begin()
        tx.create(DataTransformer.renameLabels(self._cast_json()))
        tx.commit()


class DataTransformer(object):
    @classmethod
    def renameLabels(cls, subgraph):
        for node in subgraph.nodes:
            for label in node.labels:
                if label.startswith("Bibref"):
                    node.remove_label(label)
                    node.add_label("Bibref")
                if label.startswith("Figref"):
                    node["_id"] = label
                    node.remove_label(label)
                    node.add_label("Figref")
                if label.startswith("Tabref"):
                    node["_id"] = label
                    node.remove_label(label)
                    node.add_label("Tabref")
        return subgraph

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


class IndexCreator(object):
    @classmethod
    def create(cls):
        schema = Schema(GRAPH)


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


def start():
    for datadir in DATA_DIRS:
        pth = os.path.join(DATA_BASE_DIR, datadir)

        workers = Worker.generate_workers(pth)
        for w in workers:
            w.start()

        for w in workers:
            # i dont know exactly what i am doing here. CargoCult. Take a deep dive when time is available
            w.join()


start()
