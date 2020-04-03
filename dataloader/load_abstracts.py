import os
import pandas
import hashlib
import json
from Configs import getConfig
from py2neo import Graph, NodeMatcher, Node, Subgraph
from graphio import NodeSet, RelationshipSet
import pprint

config = getConfig()
graph = Graph(config.NEO4J_CON)
# TODO: This class needs a major refactor
class Metadataloader(object):
    paper_nodes = None
    stats = None
    nodeSets = None
    relSets = None
    attr_columns = None
    id_columns = None
    id_node_label = None
    graph = None

    def __init__(self, metadata_file, graph):
        self.graph = graph
        self.stats = {
            "sha-miss": {
                "desc": "SHAs from metadatafile not existent in json data set",
                "count": 0,
            },
            "attrs-added": {
                "desc": "Attrbibutes added from metadata filed not existent in json data set",
                "count": 0,
                "per-attr": {},
            },
            "non-full-paper-added": {
                "desc": "Paper added from the metadata.csv not having a fulltext json dataset counterpart",
                "count": 0,
            },
        }

        # Id columns in metadata to node map
        # {column-name-in-metadata:{"label":target-node-label,"attr":target-node-attr}}
        self.id_columns = {
            "doi": {"label": "Doi", "attr": "id"},
            "pmcid": {"label": "Pmcid", "attr": "id"},
            "pubmed_id": {"label": "Pmid", "attr": "id"},
            "Microsoft Academic Paper ID": {
                "label": "MsAcademicPaperID",
                "attr": "id",
            },
            "WHO #Covidence": {"label": "WHO_Covidence", "attr": "id"},
        }

        self.attr_columns = [
            "publish_time",
            "title",
            "journal",
            "source_x",
            "license",
        ]
        self.id_node_label = "PaperID"

        # Create entries for attr statistics
        for attr in self.attr_columns:
            self.stats["attrs-added"]["per-attr"][attr] = 0

        self._define_node_and_relatinship_sets()
        self._fetchExistingPaperNodes()
        self.data = pandas.read_csv(metadata_file)

    def define_graph(self):
        data_len = len(self.data)
        for index, row in self.data.iterrows():
            print("Row {} of {}".format(index + 1, data_len))
            if pandas.isna(row["sha"]):
                # we do not have a fulltext paper node in the db
                self.create_metadata_only_nodes(row)
            else:

                paper_node = self._findExistingPaperNode(row["sha"])
                if paper_node is None:
                    self.stats["sha-miss"]["count"] += 1
                    self.create_metadata_only_nodes(row)
                else:
                    # Set attributes from metadata file if data is missing in node
                    for attr in self.attr_columns:
                        if not pandas.isna(row[attr]) and paper_node[attr] is None:
                            paper_node[attr] = row[attr]
                            self.stats["attrs-added"]["count"] += 1
                            self.stats["attrs-added"]["per-attr"][attr] += 1
                    if not paper_node["has_abstract"]:
                        self.create_abstract_node(
                            paper_node["paper_id"], row["abstract"]
                        )
                    # remove the 'has not abstract'-flag as we dont want to have it in the graph
                    del paper_node["has_abstract"]
                    self.nodeSets["Papers"].nodes.append(paper_node)
                    self._create_id_nodes(row, paper_node["paper_id"])

    def _define_node_and_relatinship_sets(self):
        # Define nodesets
        nodeSets = {}
        relSets = {}
        nodeSets["Papers"] = NodeSet(["Paper"], ["paper_id"])
        nodeSets["PaperIDHubs"] = NodeSet(
            [self.id_node_label, config.JSON2GRAPH_COLLECTION_NODE_LABEL], ["id"]
        )
        nodeSets["Metadata"] = NodeSet(["Metadata"], ["_hash_id"])
        nodeSets["Authors"] = NodeSet(["Author"], ["_hash_id"])
        nodeSets["AuthorHubs"] = NodeSet(
            ["Author", config.JSON2GRAPH_COLLECTION_NODE_LABEL], ["id"]
        )
        nodeSets["Abstracts"] = NodeSet(["Abstract"], ["_hash_id"])
        nodeSets["AbstractHubs"] = NodeSet(
            ["Abstract", config.JSON2GRAPH_COLLECTION_NODE_LABEL], ["id"]
        )

        relSets["PAPER_HAS_PAPERID_COLLECTION"] = RelationshipSet(
            rel_type="PAPER_HAS_PAPERID_COLLECTION",
            start_node_labels=["Paper"],
            end_node_labels=[
                self.id_node_label,
                config.JSON2GRAPH_COLLECTION_NODE_LABEL,
            ],
            start_node_properties=["paper_id"],
            end_node_properties=["id"],
        )

        relSets["PAPER_HAS_METADATA"] = RelationshipSet(
            rel_type="PAPER_HAS_METADATA",
            start_node_labels=["Paper"],
            end_node_labels=["Metadata"],
            start_node_properties=["paper_id"],
            end_node_properties=["_hash_id"],
        )

        relSets["METADATA_HAS_AUTHORHUB"] = RelationshipSet(
            rel_type="METADATA_HAS_AUTHOR",
            start_node_labels=["Metadata"],
            end_node_labels=["Author", config.JSON2GRAPH_COLLECTION_NODE_LABEL],
            start_node_properties=["_hash_id"],
            end_node_properties=["id"],
        )
        relSets["AUTHORHUB_HAS_AUTHOR"] = RelationshipSet(
            rel_type="AUTHOR_HAS_AUTHOR",
            start_node_labels=["Author", config.JSON2GRAPH_COLLECTION_NODE_LABEL],
            end_node_labels=["Author"],
            start_node_properties=["id"],
            end_node_properties=["_hash_id"],
        )

        relSets["PAPER_HAS_ABSTRACTHUB"] = RelationshipSet(
            rel_type="PAPER_HAS_ABSTRACT",
            start_node_labels=["Paper"],
            end_node_labels=["Abstract", config.JSON2GRAPH_COLLECTION_NODE_LABEL],
            start_node_properties=["paper_id"],
            end_node_properties=["id"],
        )
        relSets["ABSTRACTHUB_HAS_ABSTRACT"] = RelationshipSet(
            rel_type="ABSTRACT_HAS_ABSTRACT",
            start_node_labels=["Abstract", config.JSON2GRAPH_COLLECTION_NODE_LABEL],
            end_node_labels=["Abstract"],
            start_node_properties=["id"],
            end_node_properties=["_hash_id"],
        )

        # Define id nodes and relations
        for col_name, node_props in self.id_columns.items():
            nodeSets[node_props["label"]] = NodeSet(
                [self.id_node_label, node_props["label"]], ["id"]
            )
            relSets[node_props["label"]] = RelationshipSet(
                rel_type="PAPERID_COLLECTION_HAS_PAPERID",
                start_node_labels=[
                    self.id_node_label,
                    config.JSON2GRAPH_COLLECTION_NODE_LABEL,
                ],
                end_node_labels=[self.id_node_label, node_props["label"]],
                start_node_properties=["id"],
                end_node_properties=[node_props["attr"]],
            )

        self.nodeSets = nodeSets
        self.relSets = relSets

    def _findExistingPaperNode(self, sha, remove_found_papers=True):
        if self.paper_nodes is None:
            return None
        for paper_node in self.paper_nodes.nodes:
            if paper_node["paper_id"] == sha:
                # remove node to accelarte future searches
                if remove_found_papers:
                    self.paper_nodes = self.paper_nodes - paper_node
                return paper_node
        return None

    def _fetchExistingPaperNodes(self):
        # This function is so ugly.  I can hardly stand it.
        # TODO: Refactor!!!
        print("Fetch all :Paper nodes")
        tx = graph.begin()
        # Find :Paper node that is matching metadata file row
        # NodeMatcher is broken in current py2neo stable version
        # https://github.com/technige/py2neo/issues/791
        # matcher = NodeMatcher(tx)
        # paper_node = matcher.match("Paper").first()
        self.paper_nodes = None
        # Get all :Papers with abstract
        paper_nodes_with_abstract = tx.run(
            "MATCH (n:Paper)-[:PAPER_HAS_ABSTRACT]->() RETURN n"
        ).to_subgraph()
        # Add a flag to the papers as having a abstarct
        if not paper_nodes_with_abstract is None:
            for node in paper_nodes_with_abstract.nodes:
                node["has_abstract"] = True
                if self.paper_nodes is None:
                    self.paper_nodes = node
                else:
                    self.paper_nodes = self.paper_nodes | node

        # get the rest of the papers
        papers_with_no_ab = tx.run(
            "MATCH (p:Paper) WHERE NOT (p)-[:PAPER_HAS_ABSTRACT]->() RETURN p"
        ).to_subgraph()
        if not papers_with_no_ab is None:
            for node in papers_with_no_ab.nodes:
                node["has_abstract"] = False
                self.paper_nodes = self.paper_nodes | node
        tx.finish()
        if self.paper_nodes is None:
            l = 0
        else:
            l = len(self.paper_nodes.nodes)
        print("Successfull fetched {} :Paper nodes.".format(l))

    def _create_id_nodes(self, row, paper_id):
        id_count = 0
        for col_name, node_props in self.id_columns.items():
            if not pandas.isna(row[col_name]):
                id_count = +1
                self.nodeSets[node_props["label"]].add_node(
                    {node_props["attr"]: row[col_name]}
                )
                self.relSets[node_props["label"]].add_relationship(
                    {"id": paper_id}, {node_props["attr"]: row[col_name]}, {},
                )

        if id_count > 0:
            self.nodeSets["PaperIDHubs"].add_node({"id": paper_id})
            self.relSets["PAPER_HAS_PAPERID_COLLECTION"].add_relationship(
                {"paper_id": paper_id}, {"id": paper_id}, {},
            )

    def create_metadata_only_nodes(self, row):
        def create_author_node(name_str):
            last, first, middle = None, None, None
            try:
                last, first = name_str.split(",")
            except ValueError:
                last = name_str
            if first is not None:
                try:
                    first, middle = first.split(" ")
                except ValueError:
                    pass
            hash_id = hashlib.md5(name_str.encode()).hexdigest()
            self.nodeSets["Authors"].add_node(
                {"_hash_id": hash_id, "first": first, "last": last, "middle": middle}
            )

            return hash_id

        def create_author_collection(author_ids):
            _id = hashlib.md5("".join(author_ids).encode()).hexdigest()
            self.nodeSets["AuthorHubs"].add_node({"id": _id})
            for author_id in author_ids:
                self.relSets["AUTHORHUB_HAS_AUTHOR"].add_relationship(
                    {"id": _id}, {"_hash_id": author_id}, {}
                )
            return _id

        self.stats["non-full-paper-added"]["count"] += 1
        paper_node_props = {}
        for attr in self.attr_columns:
            if not pandas.isna(row[attr]):
                paper_node_props[attr] = row[attr]
        # Create hash id based on props
        paper_node_props["paper_id"] = hashlib.md5(
            json.dumps(paper_node_props).encode()
        ).hexdigest()
        # Add :Abstract Node
        self.create_abstract_node(paper_node_props["paper_id"], row["abstract"])
        # Add :Metadata with :Author nodes
        self.nodeSets["Metadata"].add_node({"_hash_id": paper_node_props["paper_id"]})
        self.relSets["PAPER_HAS_METADATA"].add_relationship(
            {"paper_id": paper_node_props["paper_id"]},
            {"_hash_id": paper_node_props["paper_id"]},
            {},
        )

        if not pandas.isna(row["authors"]):
            author_ids = []
            for author in row["authors"].split(";"):
                author_ids.append(create_author_node(author.strip()))
            author_hub_id = create_author_collection(author_ids)
            self.relSets["METADATA_HAS_AUTHORHUB"].add_relationship(
                {"_hash_id": paper_node_props["paper_id"]}, {"id": author_hub_id}, {}
            )
        self.nodeSets["Papers"].add_node(paper_node_props)
        self._create_id_nodes(row, paper_node_props["paper_id"])

    def create_abstract_node(self, paper_id, text):
        if not pandas.isna(text) and text != "Unknown":
            self.nodeSets["AbstractHubs"].add_node({"id": paper_id})
            abstract_id = hashlib.md5("".join(text).encode()).hexdigest()
            self.relSets["PAPER_HAS_ABSTRACTHUB"].add_relationship(
                {"paper_id": paper_id}, {"id": paper_id}, {},
            )
            self.nodeSets["Abstracts"].add_node({"_hash_id": abstract_id, "text": text})
            self.relSets["ABSTRACTHUB_HAS_ABSTRACT"].add_relationship(
                {"id": paper_id}, {"_hash_id": abstract_id}, {}
            )

    def merge_graph(self):
        for nSet in self.nodeSets.values():
            nSet.merge(self.graph)
        for rSet in self.relSets.values():
            rSet.merge(self.graph)


def load_abstracts():

    metaloader = Metadataloader(config.METADATA_FILE, graph)
    metaloader.define_graph()

    metaloader.merge_graph()
    print(metaloader.stats)


if __name__ == "__main__":
    load_abstracts()
