"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, unused-import, undefined-variable, too-many-nested-blocks, duplicate-code

import os
import pathlib
from typing import cast

from core_utils.article.article import (
    Article,
    ArtifactType,
    get_article_id_from_filepath,
)
from core_utils.article.io import from_meta, from_raw, to_cleaned
from core_utils.constants import ASSETS_PATH, PROJECT_ROOT
from core_utils.pipeline import (
    LibraryWrapper,
    PipelineProtocol,
    TreeNode,
)
import spacy_udpipe
from spacy_conll import init_parser

MODEL_PATH = PROJECT_ROOT / "lab_6_pipeline" / "assets" / "model"
MODEL_NAME = "russian-syntagrus-ud-2.0-170801.udpipe"

class EmptyDirectoryError(Exception):
    """
    Raised when directory is empty.
    """

class InconsistentDatasetError(Exception):
    """
    Raised when the dataset has structural issues (missing files, gaps, etc.).
    """

class EmptyFileError(Exception):
    """
    Raised when a file is empty.
    """

try:
    from networkx import DiGraph
    from networkx.algorithms.isomorphism import DiGraphMatcher
except ImportError:
    DiGraph = None  # type: ignore
    print("No libraries installed. Failed to import.")

try:
    from spacy.language import Language
    from spacy.tokens import Doc
except ImportError:
    Language = None  # type: ignore
    Doc = None  # type: ignore
    print("No libraries installed. Failed to import.")


class CorpusManager:
    """
    Work with articles and store them.
    """

    def __init__(self, path_to_raw_txt_data: pathlib.Path) -> None:
        """
        Initialize an instance of the CorpusManager class.

        Args:
            path_to_raw_txt_data (pathlib.Path): Path to raw txt data
        """
        self.path_to_raw_txt_data = path_to_raw_txt_data
        self._storage = {}
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path_to_raw_txt_data.exists():
            raise FileNotFoundError(
                "Path does not exist."
            )
        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError(
                "Path does not lead to a directory."
            )

        files = list(self.path_to_raw_txt_data.iterdir())
        if not files:
            raise EmptyDirectoryError(
                "Directory is empty."
                )

        raw_files = {}
        meta_files = {}
        for file in files:
            name = file.name
            if name.endswith("_raw.txt"):
                raw_files[int(name.replace("_raw.txt", ""))] = file
            elif name.endswith("_meta.json"):
                meta_files[int(name.replace("_meta.json", ""))] = file

        if not raw_files:
            raise InconsistentDatasetError(
                "Dataset contains no raw files."
            )

        raw_ids = sorted(raw_files.keys())
        if raw_ids != list(range(1, max(raw_ids) + 1)):
            raise InconsistentDatasetError(
                "IDs contain slips."
            )
        
        for file in raw_files.values():
            if file.stat().st_size == 0:
                raise InconsistentDatasetError(
                    f"Raw file {file.name} is empty"
                )
        if meta_files:
            for file in meta_files.values():
                if file.stat().st_size == 0:
                    raise InconsistentDatasetError(f"Meta file {file.name} is empty")
            if set(raw_files.keys()) != set(meta_files.keys()):
                raise InconsistentDatasetError("Raw and meta ids are unequal.")


    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for meta_file_path in self.path_to_raw_txt_data.glob("*_meta.json"):
            article_id = get_article_id_from_filepath(meta_file_path)
            self._storage[article_id] = from_meta(meta_file_path, Article(None, article_id))
        for raw_file_path in self.path_to_raw_txt_data.glob("*_raw.txt"):
            article_id = get_article_id_from_filepath(raw_file_path)
            self._storage[article_id] = from_raw(raw_file_path, self._storage[article_id])



    def get_articles(self) -> dict:
        """
        Get storage params.

        Returns:
            dict: Storage params
        """
        return self._storage


class TextProcessingPipeline(PipelineProtocol):
    """
    Preprocess and morphologically annotate sentences into the CONLL-U format.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper | None = None
    ) -> None:
        """
        Initialize an instance of the TextProcessingPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper | None, optional): Analyzer instance. Defaults to None.
        """
        self._corpus = corpus_manager
        self._analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        articles = self._corpus.get_articles()
        for article in articles.values():
            raw_text = article.text
            punctuation = r'!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
            translator = str.maketrans('', '', punctuation)
            cleaned = raw_text.lower().translate(translator)
            cleaned = ' '.join(cleaned.split())
            article._cleaned = cleaned
            to_cleaned(article)
            if self._analyzer:
                conllu_list = self._analyzer.analyze([raw_text])
                if conllu_list and len(conllu_list) > 0 and conllu_list[0] is not None:
                    article.set_conllu_info(conllu_list[0])
                    self._analyzer.to_conllu(article)


class UDPipeAnalyzer(LibraryWrapper):
    """
    Wrapper for udpipe library.
    """

    #: Analyzer
    _analyzer: Language

    def __init__(self) -> None:
        """
        Initialize an instance of the UDPipeAnalyzer class.
        """
        self._analyzer = self._bootstrap()

    def _bootstrap(self) -> Language:
        """
        Load and set up the UDPipe model.

        Returns:
            Language: Analyzer instance
        """
        nlp = spacy_udpipe.load("ru")
        nlp = init_parser(nlp, "conllu")
        return nlp


    def analyze(self, texts: list[str]) -> list[str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[str]: List of documents
        """
        conllu_docs = []
        for text in texts:
            doc = self._analyzer(text)
            conllu_docs.append(doc._.conllu)
        return conllu_docs

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        with open(article.get_file_path(ArtifactType.UDPIPE_CONLLU), "w", encoding="utf-8") as f:
            f.write(article.get_conllu_info())

    def from_conllu(self, article: Article) -> Doc:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            Doc: Document ready for parsing
        """


class POSFrequencyPipeline:
    """
    Count frequencies of each POS in articles, update meta info and produce graphic report.
    """

    def __init__(self, corpus_manager: CorpusManager, analyzer: LibraryWrapper) -> None:
        """
        Initialize an instance of the POSFrequencyPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
        """

    def _count_frequencies(self, article: Article) -> dict[str, int]:
        """
        Count POS frequency in Article.

        Args:
            article (Article): Article instance

        Returns:
            dict[str, int]: POS frequencies
        """

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """


class PatternSearchPipeline(PipelineProtocol):
    """
    Search for the required syntactic pattern.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper, pos: tuple[str, ...]
    ) -> None:
        """
        Initialize an instance of the PatternSearchPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
            pos (tuple[str, ...]): Root, Dependency, Child part of speech
        """

    def _make_graphs(self, doc: Doc) -> list[DiGraph]:
        """
        Make graphs for a document.

        Args:
            doc (Doc): Document for patterns searching

        Returns:
            list[DiGraph]: Graphs for the sentences in the document
        """

    def _add_children(
        self, graph: DiGraph, subgraph_to_graph: dict, node_id: int, tree_node: TreeNode
    ) -> None:
        """
        Add children to TreeNode.

        Args:
            graph (DiGraph): Sentence graph to search for a pattern
            subgraph_to_graph (dict): Matched subgraph
            node_id (int): ID of root node of the match
            tree_node (TreeNode): Root node of the match
        """

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """


def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    corpus_manager = CorpusManager(path_to_raw_txt_data=ASSETS_PATH)
    udpipe_analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(corpus_manager, udpipe_analyzer)
    pipeline.run()


if __name__ == "__main__":
    main()
