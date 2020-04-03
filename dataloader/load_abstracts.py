import os
import pandas
import pydash
import hashlib
import json
from Configs import getConfig
from py2neo import Graph, NodeMatcher, Node, Subgraph
from graphio import NodeSet, RelationshipSet
import pprint


json_files_index = None
config = getConfig()
graph = Graph(config.NEO4J_CON)


class FullTextPaperJsonFilesIndex(object):
    _index = None

    def __init__(self, base_path):
        self._index = {}
        self.base_path = base_path
        self._index_dirs(self.base_path)

    def _index_dirs(self, path):
        for root, dirs, files in os.walk(path):
            for file in files:
                # Each full text paper in the CORD-19 dataset comes as a file in json format.
                # The filename is made of the paper id (a sha hash of the origin pdf)
                file_id = os.path.splitext(os.path.basename(file))
                self._index[file_id] = os.path.join(root, file)

    def get_full_text_paper_path(self, paper_id):
        if paper_id is None:
            return None
        return self._index[id]


# ToDo:
# * Create a option to bootstrap a paper only by json, for supplement papers. hang these supplemental papers on the main paper
# * Take over all row attributes
# * create abstract
# * create body text
# * check what is missing


class Paper(object):
    self.__raw_data_json = None
    self.__raw_data_csv_row = None
    self.Author = None
    self.PaperID = None
    self.References = None
    self.BodyText = None

    @classmethod
    def from_metadata_csv_row(cls, row: pandas.Series):
        # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,author,journal,microsoft_academic_id,who_covidence,has_full_text,full_text_file,url
        paper = cls()
        full_text_paper_ids = [pid.strip() for pid in row["sha"].split(";")]

        paper.paper_id = full_text_paper_ids[0] if full_text_paper_ids else None
        paper.supplemental_paper_ids = full_text_paper_ids[1:]
        paper.PaperID = []
        paper.References = []
        paper.BodyText = []
        paper.__raw_data_csv_row = row
        paper._load_full_json()
        return paper

    def _load_full_json(self):
        self.full_text_source_file_path = None
        self.full_text_source_file_path = json_files_index.get_full_text_paper_path(
            self.paper_id
        )
        if self.full_text_source_file_path is not None:
            json_data = None
            with open(self.full_text_source_file_path) as json_file:
                json_data = json.load(json_file)
            self.__raw_data_json = json_data

    def _load_full_supplemental_json(self):
        for sup_paper in self.supplemental_paper_ids:
            self.supplement_paper


class PaperParser(object):
    def __init__(self, paper: Paper):
        self.paper = paper
        self.parse_paper_ids()
        self.parse_authors()
        self.parse_references()

    def parse_authors(self):
        def parse_author_row(paper_row):
            authors_cell = paper_row["author"]
            authors = []
            if pandas.isna(authors_cell):
                return authors
            for author_str in authors_cell.split(";"):
                author = {"last": None, "first": None, "middle": None}
                try:
                    author["last"], author["first"] = author.split(",")
                except ValueError:
                    last = author_str
                if author["first"] is not None:
                    try:
                        author["fist"], author["middle"] = author["first"].split(" ")
                    except ValueError:
                        pass
                authors.append(author)
            return authors

        # First we check if there is json data of authors, which is more detailed and formated(+pre splitted,+affiliation,+location)
        try:
            self.paper.Author = paper.__raw_data_json["metadata"]["authors"]
        except KeyError:
            # if not we will parse the author string in the metadata.csv row
            self.paper.Author = self.parse_author_row(self.paper.__raw_data_csv_row)

    def _normalize_paper_id_name(self, paper_id_name):
        for (
            correct_format,
            occurent_format,
        ) in config.PAPER_ID_NAME_NORMALISATION.items():
            if paper_id_name in occurent_format or paper_id_name == correct_format:
                return correct_format
        return paper_id_name

    def parse_paper_ids(self):
        for id_col in config.METADATA_FILE_ID_COLUMNS:
            paper_id_name = self._normalize_paper_id_name(id_col)
            self.paper.PaperID.append(
                {paper_id_name: self.paper.__raw_data_csv_row[id_col]}
            )

    def parse_references(self):
        refs = []
        try:
            raw_refs = paper.__raw_data_json["metadata"]["bib_entries"]
        except KeyError:
            return refs
        for ref_name, ref_attrs in raw_refs.items():
            ref = {"name": ref_name}
            ref["PaperID"] = []
            for key, val in ref_attrs.items():
                if isinstance(val, (str, int)):
                    ref[key] = val
            if "other_ids" in ref_attrs:
                for id_type, id_vals in ref_attrs["other_ids"].items():
                    paper_id_name = self._normalize_paper_id_name(id_type)
                    for id_val in id_vals:
                        ref["PaperID"].append({paper_id_name: id_val})
        self.paper.References = refs

    def parse_body_text(self):
        body_text = None
        self.paper.BodyText.append(body_text)

    def parse_abstract(self):
        pass


class Dataloader(object):
    def __init__(self, metadata_csv_path, dataset_dir_path):
        self.data = pandas.read_csv(metadata_csv_path)
        self.data.rename(columns=config.METADATA_FILE_COLUMN_OVERRIDE)
        json_files_index = FullTextPaperJsonFilesIndex(dataset_dir_path)

    def parse(self):
        for index, row in self.data.iterrows():
            paper = Paper.from_metadata_csv_row(row)


def load_abstracts():

    dataloader = Dataloader(config.METADATA_FILE, config.DATA_BASE_DIR)
    dataloader.parse()


if __name__ == "__main__":
    load_abstracts()

"""
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
"""
