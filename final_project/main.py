"""
Final project implementation.
"""

import sys
from pathlib import Path

# pylint: disable=unused-import
from lab_6_pipeline.pipeline import UDPipeAnalyzer

PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)




def main(corpus_path: Path, dist_path: Path) -> None:
    """
    Generate conllu file for provided corpus of texts.

    Args:
        corpus_path (Path): Path to folder containing text files.
        dist_path (Path): Path to folder for saving auto_annotated.conllu.
    """
    if not corpus_path.exists() or not corpus_path.is_dir():
        raise FileNotFoundError(f"Corpus directory does not exist: {corpus_path}")

    txt_files = list(corpus_path.glob("*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found in {corpus_path}")

    full_text = []
    for txt_file in sorted(txt_files):
        full_text.append(txt_file.read_text(encoding="utf-8"))
    full_text = "\n".join(full_text)

    analyzer = UDPipeAnalyzer()
    raw_result = analyzer.analyze(full_text)

    if isinstance(raw_result, list):
        temp = "\n".join(str(line) for line in raw_result)
        import re
        conllu_result = re.sub(r'\n{3,}', '\n\n', temp)
    else:
        conllu_result = str(raw_result)

    if not conllu_result.strip():
        raise ValueError("UDPipe analysis returned empty result")

    lines = conllu_result.splitlines()
    new_lines = []
    sent_counter = 1
    for line in lines:
        if line.startswith("# sent_id ="):
            new_lines.append(f"# sent_id = {sent_counter}")
            sent_counter += 1
        else:
            new_lines.append(line)
    conllu_result = "\n".join(new_lines)

    conllu_result = conllu_result.rstrip() + "\n\n"

    dist_path.mkdir(exist_ok=True, parents=True)
    output_file = dist_path / "auto_annotated.conllu"
    output_file.write_text(conllu_result, encoding="utf-8")


if __name__ == "__main__":
    corpus = Path(__file__).parent / "assets" / "articles"
    dist = Path(__file__).parent / "dist"
    main(corpus, dist)
