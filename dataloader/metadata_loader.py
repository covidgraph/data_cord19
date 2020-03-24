import os
import pandas
from Configs import getConfig
from py2neo import Graph, NodeMatcher, Node
from graphio import NodeSet, RelationshipSet
import pprint

config = getConfig()
graph = Graph(config.NEO4J_CON)

metadata_file = config.METADATA_FILE


def _findPaperNode(paper_nodes, sha):
    for paper_node in paper_nodes.nodes:
        if paper_node["paper_id"] == sha:
            # remove node to accelarte future searches
            paper_nodes = paper_nodes - paper_node
            return paper_nodes, paper_node
    return paper_nodes, None


def load():

    stats = {
        "sha-miss": {
            "desc": "SHAs from metadatafile not existent in json data set",
            "count": 0,
        },
        "attrs-added": {
            "desc": "Attrbibutes added from metadata filed not existent in json data set",
            "count": 0,
            "per-attr": {},
        },
    }

    attr_columns = ["publish_time", "title", "journal", "source_x"]
    id_node_label = "PaperID"

    # Define nodesets
    nodeSets = {}
    nodeSets["Papers"] = NodeSet(["Paper"], ["paper_id"])
    nodeSets["PaperIDHubs"] = NodeSet(
        [id_node_label, config.JSON2GRAPH_COLLECTION_NODE_LABEL], ["id"]
    )

    # Id columns in metadata to node map
    # {column-name-in-metadata:{"label":target-node-label,"attr":target-node-attr}}
    id_columns = {
        "doi": {"label": "Doi", "attr": "id"},
        "pmcid": {"label": "Pmcid", "attr": "id"},
        "pubmed_id": {"label": "Pmid", "attr": "id"},
        "Microsoft Academic Paper ID": {"label": "MsAcademicPaperID", "attr": "id",},
    }

    relSets = {}

    # Define relations

    relSets["PAPER_HAS_PAPERID_COLLECTION"] = RelationshipSet(
        rel_type="PAPER_HAS_PAPERID_COLLECTION",
        start_node_labels=["Paper"],
        end_node_labels=[id_node_label, config.JSON2GRAPH_COLLECTION_NODE_LABEL],
        start_node_properties=["paper_id"],
        end_node_properties=["id"],
    )

    # Define id nodes and relations
    for col_name, node_props in id_columns.items():
        nodeSets[node_props["label"]] = NodeSet(
            [id_node_label, node_props["label"]], ["id"]
        )
        relSets[node_props["label"]] = RelationshipSet(
            rel_type="PAPERID_COLLECTION_HAS_PAPERID",
            start_node_labels=[id_node_label, config.JSON2GRAPH_COLLECTION_NODE_LABEL,],
            end_node_labels=[id_node_label, node_props["label"]],
            start_node_properties=["id"],
            end_node_properties=[node_props["attr"]],
        )

    # Create entries for attr statistics
    for attr in attr_columns:
        stats["attrs-added"]["per-attr"][attr] = 0

    # fetching paper nodes
    print("Fetch all :Paper nodes")
    tx = graph.begin()
    # Find :Paper node that is matching metadata file row
    # NodeMatcher is broken in current py2neo stable version
    # https://github.com/technige/py2neo/issues/791
    # matcher = NodeMatcher(tx)
    # paper_node = matcher.match("Paper").first()
    paper_nodes = tx.run("MATCH (_:Paper) RETURN _").to_subgraph()
    tx.finish()
    print("Successfull fetched {} :Paper nodes.".format(len(paper_nodes.nodes)))

    data = pandas.read_csv(metadata_file)
    data_len = len(data)
    for index, row in data.iterrows():
        print("Row {} of {}".format(index + 1, data_len))
        if not pandas.isna(row["sha"]):

            paper_nodes, paper_node = _findPaperNode(paper_nodes, row["sha"])
            if paper_node is None:
                stats["sha-miss"]["count"] += 1
                continue
            # Set attributes from metadata file if data is missing in node
            for attr in attr_columns:
                if not pandas.isna(row[attr]) and paper_node[attr] is None:
                    paper_node[attr] = row[attr]
                    stats["attrs-added"]["count"] += 1
                    stats["attrs-added"]["per-attr"][attr] += 1
            nodeSets["Papers"].nodes.append(paper_node)

            # create id collection hub

            id_count = 0
            for col_name, node_props in id_columns.items():
                if not pandas.isna(row[col_name]):
                    id_count = +1
                    nodeSets[node_props["label"]].add_node(
                        {node_props["attr"]: row[col_name]}
                    )
                    relSets[node_props["label"]].add_relationship(
                        {"id": paper_node["paper_id"]},
                        {node_props["attr"]: row[col_name]},
                        {},
                    )

            if id_count > 0:
                nodeSets["PaperIDHubs"].add_node({"id": paper_node["paper_id"]})
                relSets["PAPER_HAS_PAPERID_COLLECTION"].add_relationship(
                    {"paper_id": paper_node["paper_id"]},
                    {"id": paper_node["paper_id"]},
                    {},
                )

                # add ids to the paper node

    # Write to DB

    for nSet in nodeSets.values():
        nSet.merge(graph)
    for rSet in relSets.values():
        # print(rSet.relationships)
        rSet.merge(graph)
    pp = pprint.PrettyPrinter(indent=4)

    pp.pprint(stats)
    # print(stats)


if __name__ == "__main__":
    load()
