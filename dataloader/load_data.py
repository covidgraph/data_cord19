import os
import json
import pathlib
import logging
from py2neo.database import ClientError
from linetimer import CodeTimer
from Configs import getConfig
from py2neo import Graph, Schema
from DZDjson2GraphIO import Json2graphio

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))

GRAPH = Graph(config.NEO4J_CON)


class DataLoader(object):
    def __init__(self, jsons_file_list: list):

        self.files = jsons_file_list

        c = Json2graphio()
        c.config_dict_label_override = config.JSON2GRAPH_LABELOVERRIDE
        c.config_func_custom_relation_name_generator = DataTransformer.nameRelation
        c.config_dict_primarykey_generated_hashed_attrs_by_label = (
            config.JSON2GRAPH_GENERATED_HASH_IDS
        )
        c.config_dict_concat_list_attr = config.JSON2GRAPH_CONCAT_LIST_ATTR
        c.config_str_collection_anchor_label = config.JSON2GRAPH_COLLECTION_NODE_LABEL
        c.config_dict_primarykey_attr_by_label = config.JSON2GRAPH_ID_ATTR
        c.config_str_primarykey_generated_attr_name = (
            config.JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME
        )
        c.config_func_node_pre_modifier = DataTransformer.renameLabels
        c.config_func_node_post_modifier = DataTransformer.addExtraLabels
        c.config_dict_property_name_override = config.JSON2GRAPH_PROPOVERRIDE
        self.loader = c

    def load_json(self):
        log.info("Load {} json files in memory...".format(len(self.files)))
        for file in self.files:
            json_data = None
            with open(file) as json_file:
                json_data = json.load(json_file)
            self.loader.load_json("Paper", json_data)
        log.info("...loaded json im memory")

    def merge(self, graph: Graph):
        log.info("Load json in Neo4j DB...")
        self.loader.create_indexes(graph)
        self.loader.merge(graph)
        log.info("...Loaded json in Neo4j DB.")


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
    def addExtraLabels(cls, node):
        if node.__primarylabel__ in ["Doi", "Arxiv", "Pmcid", "Pmid"]:
            if config.JSON2GRAPH_COLLECTION_NODE_LABEL not in node.labels:
                node.add_label("PaperID")
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
        for label in config.JSON2GRAPH_GENERATED_HASH_IDS.keys():
            cls._create_constraint(label, config.JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME)
        for label, attr in config.JSON2GRAPH_ID_ATTR.items():

            cls._create_constraint(label, attr)

    @classmethod
    def _create_constraint(cls, label, attr):
        try:
            tx = GRAPH.begin()
            cypher = "CREATE CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
                label, attr
            )
            log.debug(cypher)
            tx.run(cypher)
            tx.commit()
        except ClientError:
            pass


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def load():
    GraphSchema.create_uniqueness_constraint()
    for index, datadir in enumerate(config.DATA_DIRS):
        dir_ = os.path.join(config.DATA_BASE_DIR, datadir)

        files = [filepath.absolute() for filepath in pathlib.Path(dir_).glob("**/*")]
        log.info("Start importing dir '{}'...".format(dir_))

        file_buckets = list(chunks(files, config.COMMIT_INTERVAL))
        log.info(
            "Seperating {} files in {} buckets.".format(len(files), len(file_buckets))
        )
        for bucket_index, file_bucket in enumerate(file_buckets):
            log.info(
                "Direcory {} of {}. ".format(index + 1, len(config.DATA_DIRS))
                + "File bucket {} of {} with {} files in it.".format(
                    bucket_index + 1, len(file_buckets), len(file_bucket)
                ),
            )
            loader = DataLoader(file_bucket)
            loader.load_json()
            loader.merge(GRAPH)
