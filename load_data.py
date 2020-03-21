import os
import json
import pathlib
import multiprocessing
import numpy as np
from py2neo.database import TransientError,TransactionError
import time
import random
# from graphio import NodeSet, RelationshipSet
from py2neo import Graph, Schema
from DZDjson2Graph import Json2graph

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )


GRAPH = Graph()
WORKER_COUNT = 32

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
JSON2GRAPH_GENERATED_ID_ATTR_NAME = "hash_id"


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
        c.config_bool_reduce_relation_nodes_to_pk = False
        c.config_str_primarykey_generated_attr_name = JSON2GRAPH_GENERATED_ID_ATTR_NAME
        c.config_func_node_pre_modifier = DataTransformer.renameLabels
        return c.get_subgraph("Paper")

    def load_data(self):
        self._parse_file()
        
        sg = self._cast_json()
        #self._merge(sg.nodes)
        #self._merge(sg.relationships)
        for n in sg.nodes:
            try:
                self._merge([n])
            except:
                
                print(n)
                raise
        for r in sg.relationships:
            try:
                self._merge([n])
            except:
                print(r)
                raise
    def _merge(self,objs):
        try_count = 0
        insert_running = True
        max_retry_wait_time_sec = 5
        max_retries_on_insert_errors = 10
        while insert_running:
            try:
                tx = GRAPH.begin()
                for obj in objs:
                    tx.merge(obj)
                tx.commit()
                insert_running = False
            except (TransactionError, TransientError):
                if try_count == max_retries_on_insert_errors:
                    raise
                else:
                    try_count += 1
                    waittime = (
                        random.randrange(1, max_retry_wait_time_sec, 1)
                        * try_count
                    )
                    print(
                        "Error while inserting '{}'. But relax maybe its just a NodeLock. Will retry {} times. Waiting {} seconds until next retry".format(
                            obj,
                            max_retries_on_insert_errors - try_count,
                            waittime,
                        )
                    )
                    time.sleep(waittime)


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
    def create_indexes(cls):
        
        for label in JSON2GRAPH_GENERATED_IDS.keys():
            g = GRAPH.begin()
            schema = Schema(g)
            schema.create_index(label, JSON2GRAPH_GENERATED_ID_ATTR_NAME)
            g.finish()
        for label, attr in JSON2GRAPH_ID_ATTR.items():
            g = GRAPH.begin()
            schema = Schema(g)
            schema.create_index(label, attr)
            g.finish()
        

    @classmethod
    def create_uniqueness_constraint(cls):
        for label in JSON2GRAPH_GENERATED_IDS.keys():
            g = GRAPH
            schema = Schema(g)
            print(label)
            schema.create_uniqueness_constraint(
                label, JSON2GRAPH_GENERATED_ID_ATTR_NAME
            )
            #g.finish()
        for label, attr in JSON2GRAPH_ID_ATTR.items():
            g = GRAPH
            schema = Schema(g)
            print(label)
            schema.create_uniqueness_constraint(label, attr)
            #g.finish()
        


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
        print("Create contraints/indexes")
        GraphSchema.create_uniqueness_constraint()
        print("Generate Workers")
        workers = Worker.generate_workers(pth)
        print("Start Workers")
        for w in workers:
            w.start()

        for w in workers:
            # i dont know exactly what i am doing here. CargoCult. Take a deep dive when time is available
            w.join()


start()
