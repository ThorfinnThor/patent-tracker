from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import yaml


LEGAL_SUFFIXES = {
    "inc", "inc.", "corp", "corp.", "corporation", "co", "co.", "company",
    "ltd", "ltd.", "llc", "gmbh", "ag", "sa", "bv", "nv", "plc", "pte", "kg", "kgaa"
}


def normalize_name_for_suggestions(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split(" ") if t and t not in LEGAL_SUFFIXES]
    return " ".join(tokens)


@dataclass(frozen=True)
class AssigneeMapping:
    display_name: str
    canonical_company_id: str
    notes: str = ""


def load_assignee_map(path: str) -> Dict[str, AssigneeMapping]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    assignees = raw.get("assignees", {}) or {}
    out: Dict[str, AssigneeMapping] = {}
    for assignee_id, cfg in assignees.items():
        out[str(assignee_id)] = AssigneeMapping(
            display_name=str(cfg.get("display_name", "")).strip() or str(assignee_id),
            canonical_company_id=str(cfg.get("canonical_company_id", "")).strip() or str(assignee_id),
            notes=str(cfg.get("notes", "")).strip(),
        )
    return out


def map_assignee(
    assignee_id: str,
    raw_org_name: str,
    assignee_map: Dict[str, AssigneeMapping],
) -> Tuple[str, str]:
    """
    Returns (canonical_company_id, display_name).
    Deterministic: if not mapped, canonical_company_id is assignee_id.
    """
    if assignee_id in assignee_map:
        m = assignee_map[assignee_id]
        return (m.canonical_company_id, m.display_name)
    display = (raw_org_name or "").strip() or assignee_id
    return (assignee_id, display)
