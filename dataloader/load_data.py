import os
import pandas
import json
import logging
import multiprocessing
import functools
import concurrent

import py2neo
from pebble import ProcessPool
from linetimer import CodeTimer
from Configs import getConfig
from py2neo import Graph
from dict2graph import Dict2graph


config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))
log.info("Neo4j DB connection details: {}".format(config.NEO4J))
graph = py2neo.Graph(**config.NEO4J)


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
                file_id = os.path.splitext(os.path.basename(file))[0]
                self._index[file_id] = os.path.join(root, file)

    def get_full_text_paper_pathes(self, paper_sha, paper_pmcid):
        pathes = []
        if paper_sha is None and paper_pmcid is None:
            return pathes
        if paper_pmcid is not None:
            pmcid_file_name = "{}.xml".format(paper_pmcid.upper())
            try:
                pathes.append(self._index[pmcid_file_name])
            except KeyError:
                pass
        try:
            pathes.append(self._index[paper_sha])
        except KeyError:
            pass
        return pathes


json_files_index = FullTextPaperJsonFilesIndex(config.DATA_BASE_DIR)
# ToDo:
# * (postponed for now)Create a option to bootstrap a paper only by json, for supplement papers. hang these supplemental papers on the main paper


class Paper(object):
    _raw_data_json = None
    _raw_data_csv_row = None
    properties = None

    # child objects
    Author = None
    PaperID = None
    Reference = None
    BodyText = None
    Abstract = None

    def __init__(self, row: pandas.Series):
        # cord_uid,sha,source,title,doi,pmcid,pubmed_id,license,abstract,publish_time,author,journal,microsoft_academic_id,who_covidence,has_full_text,full_text_file,url

        # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,mag_id,who_covidence_id,arxiv_id,pdf_json_files,pmc_json_files,url,s2_id
        # ug7v899j,d1aafb70c066a2068b02786f8929fd9c900897fb,PMC,"Clinical features of culture-proven Mycoplasma pneumoniae infections at King Abdulaziz University Hospital, Jeddah, Saudi Arabia",10.1186/1471-2334-1-6,PMC35282,11472636,no-cc,"OBJECTIVE: This retrospective chart review describes the epidemiology and clinical features of 40 patients with culture-proven Mycoplasma pneumoniae infections at King Abdulaziz University Hospital, Jeddah, Saudi Arabia. METHODS: Patients with positive M. pneumoniae cultures from respiratory specimens from January 1997 through December 1998 were identified through the Microbiology records. Charts of patients were reviewed. RESULTS: 40 patients were identified, 33 (82.5%) of whom required admission. Most infections (92.5%) were community-acquired. The infection affected all age groups but was most common in infants (32.5%) and pre-school children (22.5%). It occurred year-round but was most common in the fall (35%) and spring (30%). More than three-quarters of patients (77.5%) had comorbidities. Twenty-four isolates (60%) were associated with pneumonia, 14 (35%) with upper respiratory tract infections, and 2 (5%) with bronchiolitis. Cough (82.5%), fever (75%), and malaise (58.8%) were the most common symptoms, and crepitations (60%), and wheezes (40%) were the most common signs. Most patients with pneumonia had crepitations (79.2%) but only 25% had bronchial breathing. Immunocompromised patients were more likely than non-immunocompromised patients to present with pneumonia (8/9 versus 16/31, P = 0.05). Of the 24 patients with pneumonia, 14 (58.3%) had uneventful recovery, 4 (16.7%) recovered following some complications, 3 (12.5%) died because of M pneumoniae infection, and 3 (12.5%) died due to underlying comorbidities. The 3 patients who died of M pneumoniae pneumonia had other comorbidities. CONCLUSION: our results were similar to published data except for the finding that infections were more common in infants and preschool children and that the mortality rate of pneumonia in patients with comorbidities was high.",2001-07-04,"Madani, Tariq A; Al-Ghamdi, Aisha A",BMC Infect Dis,,,,document_parses/pdf_json/d1aafb70c066a2068b02786f8929fd9c900897fb.json,document_parses/pmc_json/PMC35282.xml.json,https://www.ncbi.nlm.nih.gov/pmc/articles/PMC35282/,
        # 02tnwd4m,6b0567729c2143a66d737eb0a2f63f2dce2e5a7d,PMC,Nitric oxide: a pro-inflammatory mediator in lung disease?,10.1186/rr14,PMC59543,11667967,no-cc,"Inflammatory diseases of the respiratory tract are commonly associated with elevated production of nitric oxide (NO•) and increased indices of NO• -dependent oxidative stress. Although NO• is known to have anti-microbial, anti-inflammatory and anti-oxidant properties, various lines of evidence support the contribution of NO• to lung injury in several disease models. On the basis of biochemical evidence, it is often presumed that such NO• -dependent oxidations are due to the formation of the oxidant peroxynitrite, although alternative mechanisms involving the phagocyte-derived heme proteins myeloperoxidase and eosinophil peroxidase might be operative during conditions of inflammation. Because of the overwhelming literature on NO• generation and activities in the respiratory tract, it would be beyond the scope of this commentary to review this area comprehensively. Instead, it focuses on recent evidence and concepts of the presumed contribution of NO• to inflammatory diseases of the lung.",2000-08-15,"Vliet, Albert van der; Eiserich, Jason P; Cross, Carroll E",Respir Res,,,,document_parses/pdf_json/6b0567729c2143a66d737eb0a2f63f2dce2e5a7d.json,document_parses/pmc_json/PMC59543.xml.json,https://www.ncbi.nlm.nih.gov/pmc/articles/PMC59543/,
        # ejv2xln0,06ced00a5fc04215949aa72528f2eeaae1d58927,PMC,Surfactant protein-D and pulmonary host defense,10.1186/rr19,PMC59549,11667972,no-cc,"Surfactant protein-D (SP-D) participates in the innate response to inhaled microorganisms and organic antigens, and contributes to immune and inflammatory regulation within the lung. SP-D is synthesized and secreted by alveolar and bronchiolar epithelial cells, but is also expressed by epithelial cells lining various exocrine ducts and the mucosa of the gastrointestinal and genitourinary tracts. SP-D, a collagenous calcium-dependent lectin (or collectin), binds to surface glycoconjugates expressed by a wide variety of microorganisms, and to oligosaccharides associated with the surface of various complex organic antigens. SP-D also specifically interacts with glycoconjugates and other molecules expressed on the surface of macrophages, neutrophils, and lymphocytes. In addition, SP-D binds to specific surfactant-associated lipids and can influence the organization of lipid mixtures containing phosphatidylinositol in vitro. Consistent with these diverse in vitro activities is the observation that SP-D-deficient transgenic mice show abnormal accumulations of surfactant lipids, and respond abnormally to challenge with respiratory viruses and bacterial lipopolysaccharides. The phenotype of macrophages isolated from the lungs of SP-D-deficient mice is altered, and there is circumstantial evidence that abnormal oxidant metabolism and/or increased metalloproteinase expression contributes to the development of emphysema. The expression of SP-D is increased in response to many forms of lung injury, and deficient accumulation of appropriately oligomerized SP-D might contribute to the pathogenesis of a variety of human lung diseases.",2000-08-25,"Crouch, Erika C",Respir Res,,,,document_parses/pdf_json/06ced00a5fc04215949aa72528f2eeaae1d58927.json,document_parses/pmc_json/PMC59549.xml.json,https://www.ncbi.nlm.nih.gov/pmc/articles/PMC59549/,
        # 2b73a28n,348055649b6b8cf2b9a376498df9bf41f7123605,PMC,Role of endothelin-1 in lung disease,10.1186/rr44,PMC59574,11686871,no-cc,"Endothelin-1 (ET-1) is a 21 amino acid peptide with diverse biological activity that has been implicated in numerous diseases. ET-1 is a potent mitogen regulator of smooth muscle tone, and inflammatory mediator that may play a key role in diseases of the airways, pulmonary circulation, and inflammatory lung diseases, both acute and chronic. This review will focus on the biology of ET-1 and its role in lung disease.",2001-02-22,"Fagan, Karen A; McMurtry, Ivan F; Rodman, David M",Respir Res,,,,document_parses/pdf_json/348055649b6b8cf2b9a376498df9bf41f7123605.json,document_parses/pmc_json/PMC59574.xml.json,https://www.ncbi.nlm.nih.gov/pmc/articles/PMC59574/,

        self._raw_data_csv_row = row
        # some row refernce multiple json files (in form of a sha hash of the paper).
        # in most cases the files are the same (dunno why)
        # in some cases the extra papers a supplemental papers, in some cases they are reviewed version.
        # at the moment we ignore all this and only take the last refernce in list, as it usally the most recent paper and not a supplemental paper (not always :/ )
        # ToDo: Distinguish duplicate, supplemental and reviewd papers. Ignore duplicates and store the supplemental paper somehow
        self.paper_sha = None
        if not pandas.isna(row["sha"]):
            full_text_paper_id = [pid.strip() for pid in row["sha"].split(";")][-1:]
            self.paper_sha = full_text_paper_id[0] if full_text_paper_id[0] else None
        self.paper_pmcid = row["pmcid"] if not pandas.isna(row["pmcid"]) else None
        self._load_full_json()

        self.properties = {"cord19-fulltext_hash": self.paper_sha}
        self.PaperID = []
        self.Reference = []
        self.BodyText = []
        self.Abstract = []
        PaperParser(self)

    def to_dict(self):
        dic = self.properties
        # sub/child dicts
        dic["authors"] = self.Author
        dic["PaperID"] = self.PaperID
        dic["Reference"] = self.Reference
        dic["BodyText"] = self.BodyText
        dic["Abstract"] = self.Abstract
        return dic

    def _load_full_json(self):
        self.full_text_source_file_pathes = []
        self.full_text_source_file_pathes = json_files_index.get_full_text_paper_pathes(
            self.paper_sha, self.paper_pmcid
        )
        if self.full_text_source_file_pathes:
            self._raw_data_json = {}
            for path in self.full_text_source_file_pathes:
                json_data = None
                with open(path) as json_file:
                    json_data = json.load(json_file)
                self._raw_data_json.update(json_data)


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
            prop_val = self.paper._raw_data_csv_row[prop_name]
            if not pandas.isna(prop_val):
                self.paper.properties[prop_name] = prop_val

    def parse_authors(self):
        def parse_author_row(paper_row):
            authors_cell = paper_row["authors"]
            authors = []
            if pandas.isna(authors_cell):
                return authors
            for author_str in authors_cell.split(";"):
                author = {"last": None, "first": None, "middle": None}
                try:
                    author["last"], author["first"] = author_str.split(",")
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
            self.paper.Author = self.paper._raw_data_json["metadata"]["authors"]
        except (KeyError, TypeError):
            # if not we will parse the author string in the metadata.csv row
            self.paper.Author = parse_author_row(self.paper._raw_data_csv_row)

    def parse_paper_ids(self):
        for id_col in config.METADATA_FILE_ID_COLUMNS:
            paper_id_name = self._normalize_paper_id_name(id_col)
            paper_id = self.paper._raw_data_csv_row[id_col]
            if not pandas.isna(paper_id):
                self.paper.PaperID.append(
                    {"type": paper_id_name, "id": self._normalize_paper_id(paper_id),}
                )

    def parse_references(self):
        refs = []
        try:
            raw_refs = self.paper._raw_data_json["bib_entries"]
        except (KeyError, TypeError):
            return refs
        for ref_name, raw_attrs in raw_refs.items():
            ref = {"name": ref_name}
            for ref_attr_name, ref_attr_val in raw_attrs.items():
                # Save simple attributes
                if (
                    ref_attr_name in config.FULLTEXT_PAPER_BIBREF_ATTRS
                    and isinstance(ref_attr_val, (str, int))
                    and ref_attr_val != ""
                ):
                    ref[ref_attr_name] = ref_attr_val
                # save public IDs
                ref["PaperID"] = []
                if ref_attr_name == "other_ids":
                    for id_type, id_vals in ref_attr_val.items():
                        paper_id_name = self._normalize_paper_id_name(id_type)
                        for id_val in id_vals:

                            ref["PaperID"].append(
                                {
                                    "type": paper_id_name,
                                    "id": self._normalize_paper_id(id_val),
                                }
                            )
                refs.append(ref)
        self.paper.Reference = refs

    def parse_body_text(self):
        body_texts = []
        if self.paper._raw_data_json is not None:
            for body_text in self.paper._raw_data_json["body_text"]:
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
        if self.paper._raw_data_json is not None:
            if "abstract" in self.paper._raw_data_json:
                for abstract_sections in self.paper._raw_data_json["abstract"]:
                    if "cite_spans" in abstract_sections:
                        self._link_references(abstract_sections["cite_spans"])
                    # delete non needed data
                    if "eq_spans" in abstract_sections:
                        del abstract_sections["eq_spans"]
                    if "ref_spans" in abstract_sections:
                        del abstract_sections["ref_spans"]
                    self.paper.Abstract.append(abstract_sections)

        else:
            abst = self.paper._raw_data_csv_row["abstract"]
            if not pandas.isna(abst):
                self.paper.Abstract.append({"text": abst})

    def _link_references(self, ref_list):
        for ref in ref_list:
            if "ref_id" in ref:
                ref["Reference"] = self._find_reference(ref["ref_id"])
                del ref["ref_id"]

    def _normalize_paper_id_name(self, paper_id_name):
        for (
            correct_format,
            occurent_format,
        ) in config.PAPER_ID_NAME_NORMALISATION.items():
            if paper_id_name in occurent_format or paper_id_name == correct_format:
                return correct_format
        return paper_id_name

    def _normalize_paper_id(self, number):
        if isinstance(number, (int, float)):
            return str(int(number))
        return number

    def _find_reference(self, ref_name):
        for ref in self.paper.Reference:
            if ref_name == ref["name"]:
                return ref
        return ref_name


class Dataloader(object):
    def __init__(
        self, metadata_csv_path, from_row=None, to_row=None, worker_name: str = None,
    ):
        self.name = worker_name
        self.data = pandas.read_csv(metadata_csv_path)[from_row:to_row]

        self.data = self.data.rename(
            columns=config.METADATA_FILE_COLUMN_OVERRIDE, errors="raise"
        )
        self._build_loader()

    def parse(self):

        papers = []
        paper_total_count = len(self.data)

        paper_count = 0
        for index, row in self.data.iterrows():
            papers.append(Paper(row))
            if len(papers) == config.PAPER_BATCH_SIZE:
                log.info(
                    "{}Load next {} papers.".format(
                        self.name + ": " if self.name else "", len(papers)
                    )
                )
                self.load(papers)
                paper_count += len(papers)
                del papers
                papers = []
                log.info(
                    "{}Loaded {} from {} papers.".format(
                        self.name + ": " if self.name else "",
                        paper_count,
                        paper_total_count,
                    )
                )
        # load the leftovers papers
        self.load(papers)

    def load(self, papers):
        ct = CodeTimer("Convert paper to graph", silent=True, unit="s")
        with ct:
            for index, paper in enumerate(papers):
                self.loader.load_json(paper.to_dict(), "Paper")
        log.debug("Convert papers to graph took {}s".format(ct.took))

        try:
            if db_loading_lock is not None:
                db_loading_lock.acquire()
                log.info(
                    "{}Acquired DB loading lock.".format(
                        self.name + ": " if self.name else ""
                    )
                )
        except NameError:
            # we are in singlethreaded mode. no lock set
            pass
            log.debug("Load Data to DB...")
        try:
            ct = CodeTimer("Create Indexes", silent=True, unit="s")
            with ct:
                self.loader.create_indexes(graph)
            log.debug("Creating Indexes took {}s".format(ct.took))
            ct = CodeTimer("Load to DB", silent=True, unit="s")
            with ct:
                self.loader.merge(graph)
            log.debug("Loading papers to db took {}s".format(ct.took))
        finally:
            try:
                if db_loading_lock is not None:
                    log.info(
                        "{}Release DB loading lock.".format(
                            self.name + ": " if self.name else ""
                        )
                    )
                    db_loading_lock.release()
            except NameError:
                # we are in singlethreaded mode. no lock set
                log.debug("...Loaded Data to DB")
                pass

    def _build_loader(self):
        c = Dict2graph()
        # c.config_dict_label_override = config.JSON2GRAPH_LABELOVERRIDE
        # c.config_func_custom_relation_name_generator = DataTransformer.nameRelation
        c.config_dict_primarykey_generated_hashed_attrs_by_label = {
            "BodyText": "AllAttributes",
            "Paper": "AllAttributes",
            "Reference": "InnerContent",
            "Location": "AllAttributes",
            "Abstract": ["text"],  # Generate an id based on the property "text"
            "Affiliation": "AllAttributes",  # Generate an id based all properties
            "authors": "AllAttributes",
            "Citation": "AllAttributes",
        }
        c.config_dict_concat_list_attr = {"authors": {"middle": " "}}
        c.config_str_collection_hub_label = "{LIST_MEMBER_LABEL}Collection"
        c.config_list_collection_hub_extra_labels = []

        c.config_graphio_batch_size = config.COMMIT_INTERVAL
        # c.config_dict_primarykey_attr_by_label = config.JSON2GRAPH_ID_ATTR
        c.config_str_primarykey_generated_attr_name = "_hash_id"
        c.config_list_blocklist_collection_hubs = [
            "PaperIDCollection",
            "CitationCollection",
        ]
        c.config_bool_capitalize_labels = False
        c.config_dict_label_override = {
            "location": "Location",
            "cite_spans": "Citation",
            "affiliation": "Affiliation",
        }
        # c.config_func_node_pre_modifier = DataTransformer.renameLabels
        # c.config_func_node_post_modifier = DataTransformer.addExtraLabels
        # c.config_dict_property_name_override = config.JSON2GRAPH_PROPOVERRIDE
        self.loader = c


def worker_task(
    metadata_csv_path, from_row: int, to_row: int, worker_name: str,
):

    log.info("Start {} -- row {} to row {}".format(worker_name, from_row, to_row))
    # l = 1 / 0
    dataloader = Dataloader(
        metadata_csv_path, from_row=from_row, to_row=to_row, worker_name=worker_name,
    )
    dataloader.parse()


def worker_task_init(l):
    global db_loading_lock
    db_loading_lock = l


def worker_task_done(task_name, pool, other_futures, from_row, to_row, future):
    try:
        result = future.result()
    except concurrent.futures.CancelledError:
        # canceled by god or anyone
        log.info("{} cancelled".format(task_name))
        return
    except Exception as error:
        if config.CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS:
            log.warning(
                "{} failed. Cancel all tasks and stop workers...".format(task_name)
            )
            pool.close()

            for fut in other_futures:
                fut.cancel()
            future.cancel()
        log.info("{} failed".format(task_name))
        log.exception("[{}] Function raised {}".format(task_name, error))
        log.info(
            "Exception happend in {} -> row range {} - {}".format(
                config.METADATA_FILE, from_row, to_row
            )
        )
        if config.CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS:
            pool.stop()
        global exit_code
        exit_code = 1
        raise error
    log.info("{} finished".format(task_name))
    return


def load_data_mp(worker_count: int, rows_per_worker=None):
    global exit_code
    exit_code = 0
    row_count_total = len(pandas.read_csv(config.METADATA_FILE).dropna(how="all"))

    if rows_per_worker is None:
        # just distribute all rows to workers. all workers will run simulationsly
        rows_per_worker = int(row_count_total / worker_count)
        leftover_rows = row_count_total % worker_count
        worker_instances_count = worker_count
    else:
        # we create a queue of workers, only <worker_count> will run simulationsly
        worker_instances_count = int(row_count_total / rows_per_worker) or 1
        leftover_rows = row_count_total % rows_per_worker

    lock = multiprocessing.Lock()
    rows_distributed = 0
    futures = []
    with ProcessPool(
        max_workers=worker_count,
        max_tasks=1,
        initializer=worker_task_init,
        initargs=(lock,),
    ) as pool:
        for worker_index in range(0, worker_instances_count):
            from_row = rows_distributed
            rows_distributed += rows_per_worker
            if worker_index == worker_instances_count:
                # last worker gets the leftofter rows
                rows_distributed += leftover_rows
            worker_task_name = "WORKER_TASK_{}".format(worker_index)
            log.info("Add worker task '{}' to schedule".format(worker_task_name))
            future = pool.schedule(
                worker_task,
                args=(
                    config.METADATA_FILE,
                    from_row,
                    rows_distributed,
                    worker_task_name,
                ),
                # timeout=600,
            )

            future.add_done_callback(
                functools.partial(
                    worker_task_done,
                    worker_task_name,
                    pool,
                    futures,
                    from_row,
                    rows_distributed,
                )
            )
            futures.append(future)
            rows_distributed += 1
    exit(exit_code)


# pandas.read_csv(config.METADATA_FILE)

# Simple singleprocessed loading
def load_data():
    dataloader = Dataloader(config.METADATA_FILE)
    dataloader.parse()


if __name__ == "__main__":
    # with CodeTimer(unit="s"):
    load_data_mp(config.NO_OF_PROCESSES, config.PAPER_BATCH_SIZE)
    # load_data_mp(2, 1)
    # load_data()

