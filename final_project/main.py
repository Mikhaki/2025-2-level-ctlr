"""
Final project implementation.
"""

import re
import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# pylint: disable=wrong-import-position
from lab_6_pipeline.pipeline import UDPipeAnalyzer





def read_corpus_texts(corpus_path: Path) -> list[str]:
    """
    Read all .txt files from corpus folder.

    Args:
        corpus_path (Path): Path to folder containing text files.

    Returns:
        list[str]: List of texts from .txt files.
    """
    if not corpus_path.exists() or not corpus_path.is_dir():
        raise FileNotFoundError(f"Corpus directory does not exist: {corpus_path}")

    txt_files = sorted(corpus_path.glob("*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found in {corpus_path}")

    return [txt_file.read_text(encoding="utf-8") for txt_file in txt_files]

def prepare_conllu(raw_result: list[str] | str) -> str:
    """
    Prepare UDPipe result as conllu text.

    Args:
        raw_result (list[str] | str): Result returned by UDPipeAnalyzer.

    Returns:
        str: Normalized conllu text.
    """
    if isinstance(raw_result, list):
        conllu_result = "\n".join(str(line) for line in raw_result)
        return re.sub(r"\n{3,}", "\n\n", conllu_result)

    return str(raw_result)

def renumber_sent_ids(conllu_text: str) -> str:
    """
    Replace sentence ids with consecutive numbers.

    Args:
        conllu_text (str): Text in conllu format.

    Returns:
        str: Conllu text with new sentence ids.
    """
    new_lines = []
    sent_counter = 1

    for line in conllu_text.splitlines():
        if line.startswith("# sent_id ="):
            new_lines.append(f"# sent_id = {sent_counter}")
            sent_counter += 1
        else:
            new_lines.append(line)

    return "\n".join(new_lines)

def save_conllu(conllu_text: str, dist_path: Path) -> None:
    """
    Save conllu text to dist folder.

    Args:
        conllu_text (str): Text in conllu format.
        dist_path (Path): Path to output folder.
    """
    dist_path.mkdir(exist_ok=True, parents=True)

    output_file = dist_path / "auto_annotated.conllu"
    output_file.write_text(conllu_text, encoding="utf-8")

def main(corpus_path: Path, dist_path: Path) -> None:
    """
    Generate conllu file for provided corpus of texts.

    Args:
        corpus_path (Path): Path to folder containing text files.
        dist_path (Path): Path to folder for saving auto_annotated.conllu.
    """
    texts_list = read_corpus_texts(corpus_path)

    analyzer = UDPipeAnalyzer()
    raw_result = analyzer.analyze(texts_list)

    conllu_result = prepare_conllu(raw_result)

    if not conllu_result.strip():
        raise ValueError("UDPipe analysis returned empty result")

    conllu_result = renumber_sent_ids(conllu_result)
    conllu_result = conllu_result.rstrip() + "\n\n"

    save_conllu(conllu_result, dist_path)

if __name__ == "__main__":
    corpus = Path(__file__).parent / "assets" / "articles"
    dist = Path(__file__).parent / "dist"

    main(corpus, dist)
