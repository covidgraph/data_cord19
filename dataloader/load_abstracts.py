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

    def get_full_text_paper_path(self, paper_sha):
        if paper_sha is None:
            return None
        return self._index[id]


# ToDo:
# * (postponed for now)Create a option to bootstrap a paper only by json, for supplement papers. hang these supplemental papers on the main paper
# * pass papers to json2graphio and commit every <config.COMMIT_INTERVALL>


class Paper(object):
    self.__raw_data_json = None
    self.__raw_data_csv_row = None
    self.properties = None

    # child objects
    self.Author = None
    self.PaperID = None
    self.References = None
    self.BodyText = None
    self.Abstract = None

    def __init__(self, row: pandas.Series):
        # cord_uid,sha,source,title,doi,pmcid,pubmed_id,license,abstract,publish_time,author,journal,microsoft_academic_id,who_covidence,has_full_text,full_text_file,url
        self.__raw_data_csv_row = row
        # some row refernce multiple json files (in form of a sha hash of the paper).
        # in most cases the files are the same (dunno why)
        # in some cases the extra papers a supplemental papers, in some cases they are reviewed version.
        # at the moment we ignore all this and only take the last refernce in list, as it usally the most recent paper and not a supplemental paper (not always :/ )
        # ToDo: Distinguish duplicate, supplemental and reviewd papers. Ignore duplicates and store the supplemental paper somehow
        if not pandas.isna(row["sha"]):
            full_text_paper_id = [pid.strip() for pid in row["sha"].split(";")][-1:]
            self.paper_sha = full_text_paper_id[0] if full_text_paper_id[0] else None
            self._load_full_json()
        else:
            self.paper_sha = None

        self.properties = {}
        self.PaperID = []
        self.References = []
        self.BodyText = []
        self.Abstract = []
        PaperParser(self)

    def to_dict(self):
        dic = self.properties
        # sub/child dicts
        dic["Author"] = self.Author
        dic["PaperID"] = self.PaperID
        dic["References"] = self.References
        dic["BodyText"] = self.BodyText
        dic["Abstract"] = self.Abstract
        return dic

    def _load_full_json(self):
        self.full_text_source_file_path = None
        self.full_text_source_file_path = json_files_index.get_full_text_paper_path(
            self.paper_sha
        )
        if self.full_text_source_file_path is not None:
            json_data = None
            with open(self.full_text_source_file_path) as json_file:
                json_data = json.load(json_file)
            self.__raw_data_json = json_data


class PaperParser(object):
    def __init__(self, paper: Paper):
        self.paper = paper
        self.parse_paper_properties()
        self.parse_paper_ids()
        self.parse_authors()
        self.parse_references()
        self.parse_abstract()
        self.parse_body_text()

    def parse_paper_properties(self):
        for prop_name in config.METADATA_PAPER_PROPERTY_COLUMNS:
            prop_val = self.paper.from_metadata_csv_row[prop_name]
            if not pandas.isna(prop_val):
                self.paper.properties[prop_name] = prop_val

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

        # First we check if there is json data of authors, which is more detailed and allready formated(+pre splitted,+affiliation,+location)
        try:
            self.paper.Author = paper.__raw_data_json["metadata"]["authors"]
        except KeyError:
            # if not we will parse the author string in the metadata.csv row
            self.paper.Author = self.parse_author_row(self.paper.__raw_data_csv_row)

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
        for raw_ref in raw_refs:
            for ref_attr_name, ref_attr_val in raw_refs.items():
                ref = {}
                # Safe simple attributes
                if ref_attr_name in config.FULLTEXT_PAPER_BIBREF_ATTRS and isinstance(
                    ref_attr_val, (str, int)
                ):
                    ref[ref_attr_name] = ref_attr_val
                # save public IDs
                ref["PaperID"] = []
                if ref_attr_name == "other_ids":
                    for id_type, id_vals in ref_attrs["other_ids"].items():
                        paper_id_name = self._normalize_paper_id_name(id_type)
                        for id_val in id_vals:
                            ref["PaperID"].append({paper_id_name: id_val})
        self.paper.References = refs

    def parse_body_text(self):
        body_texts = []
        for body_text in self.paper.__raw_data_json["body_text"]:
            if "cite_spans" in body_text:
                self._link_references(body_text["cite_spans"])
            # delete non needed data
            if "eq_spans" in body_text:
                del body_text["eq_spans"]
            if "ref_spans" in body_text:
                del body_text["ref_spans"]
            self.paper.BodyText.append(body_text)

    def parse_abstract(self):
        abstract_sections = []
        for abstract_sections in self.paper.__raw_data_json["abstract"]:
            if "cite_spans" in abstract_sections:
                self._link_references(abstract_sections["cite_spans"])
            # delete non needed data
            if "eq_spans" in abstract_sections:
                del abstract_sections["eq_spans"]
            if "ref_spans" in abstract_sections:
                del abstract_sections["ref_spans"]
            self.paper.Abstract.append(abstract_sections)

    def _link_references(self, ref_list):
        for ref in ref_list:
            if "ref_id" in ref:
                ref["ref"] = self._find_reference(ref["ref_id"])
                del ref["ref_id"]

    def _normalize_paper_id_name(self, paper_id_name):
        for (
            correct_format,
            occurent_format,
        ) in config.PAPER_ID_NAME_NORMALISATION.items():
            if paper_id_name in occurent_format or paper_id_name == correct_format:
                return correct_format
        return paper_id_name

    def _find_reference(self, ref_name):
        for ref in self.paper.References:
            if ref_name == ref["name"]:
                return ref
        return ref_name


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
