"""Check retained ADR statuses and local Markdown evidence links."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
STATUSES = {"pending", "planning", "progress", "accepted", "rejected"}


def evaluate(root: Path = ROOT):
    errors = []
    documents = sorted(root.rglob("*.md"))
    graph = (root / "adr" / "README.md").read_text()
    states = {}
    links = 0
    for document in documents:
        relative = str(document.relative_to(root))
        text = document.read_text()
        for target in re.findall(r"\[[^\]\n]*\]\(([^)\n]+)\)", text):
            target = target.strip().strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            links += 1
            path = (document.parent / unquote(parsed.path)).resolve()
            if not path.exists():
                errors.append(f"missing_link:{relative}:{target}")
        if document.parent != root / "adr" or not document.name.startswith("ADR-"):
            continue
        match = re.search(r"^- Status: `([^`]+)`$", text, re.MULTILINE)
        status = None if match is None else match[1]
        states[document.name] = status
        if status not in STATUSES:
            errors.append("invalid_status:" + relative)
        if document.name not in graph:
            errors.append("unindexed_adr:" + relative)
        indexed = re.search(
            r"\]\(" + re.escape(document.name) + r"\).*?`([^`]+)`", graph
        )
        if indexed is None or indexed[1] != status:
            errors.append("index_status_mismatch:" + relative)
        if status == "accepted":
            for section in (
                "Problem",
                "Theory",
                "Stories",
                "Acceptance criteria",
                "Consequences",
            ):
                if not re.search(
                    r"^## " + re.escape(section) + r"\b", text, re.MULTILINE
                ):
                    errors.append(f"accepted_missing_section:{relative}:{section}")
            if not re.search(
                r"^## .*Evidence|^## Evidence", text, re.MULTILINE | re.IGNORECASE
            ):
                errors.append("accepted_missing_evidence:" + relative)
            if not re.search(
                r"As a (?:developer|user|system|operator),? I (?:want|can|got)", text
            ):
                errors.append("accepted_missing_story:" + relative)
    return {
        "passed": not errors,
        "violations": errors,
        "markdown_documents": len(documents),
        "local_links": links,
        "adr_statuses": states,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = evaluate()
    value = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output is not None:
        args.output.write_text(value)
    print(value, end="")
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
