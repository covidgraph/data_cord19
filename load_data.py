import os
import json
import pathlib
import multiprocessing
import numpy as np
from py2neo import Graph
from DZDjson2Graph import Json2graph

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )


GRAPH = Graph()
WORKER_COUNT = 2

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

JSON2GRAPH_LABELOVERRIDE = {"authors": "Author"}


class DataLoader(object):
    def __init__(self, json_path):

        self.json_path = json_path

    def _parse_file(self):
        with open(self.json_path) as json_file:
            self.data = json.load(json_file)

    def _cast_json(self):
        c = Json2graph(self.data)
        c.label_override = JSON2GRAPH_LABELOVERRIDE
        return c.get_subgraph("Paper")

    def load_data(self):
        self._parse_file()
        tx = GRAPH.begin()
        tx.create(self._cast_json())
        tx.commit()


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
            print(bucket)
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
