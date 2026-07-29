"""Name normalization, org_first_seen, and topic tagging — shared by ingest.py and publish.py."""

import json
import re

import duckdb

_PAREN_RE = re.compile(r"\([^)]*\)")
_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")

# Conservative: only strip unambiguous legal-entity suffixes, not words like "group" or
# "holdings" that can be a real part of an org's identity. Iterative so "ABC CORP INC" -> "abc".
_LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "corp", "corporation", "co", "company",
    "ltd", "llp", "lp", "pllc", "pc", "na",
}


def norm_name(name: str, strip_suffixes: bool = True) -> str:
    """Normalize an org name for cross-filing identity matching.

    Lowercases, strips trailing parentheticals and punctuation, collapses whitespace, and
    (by default) strips trailing legal-entity suffixes so "Acme Corp" and "Acme Corporation"
    collapse to the same key. Mirrors the Canadian lobbyist app's norm_name() heuristic.
    """
    if not name:
        return ""
    s = name.lower()
    s = _PAREN_RE.sub(" ", s)
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    if strip_suffixes:
        words = s.split(" ")
        while words and words[-1] in _LEGAL_SUFFIXES:
            words.pop()
        s = " ".join(words)
    return s


# Lives in web/ (not repo root) so the deployed static site can fetch the same file the
# pipeline tags with — the frontend's topic pills and the precomputed tags stay in sync.
TOPICS_PATH = "web/topics.json"


def load_topics(path: str = TOPICS_PATH) -> dict:
    with open(path) as f:
        return json.load(f)


def tag_topics(con: duckdb.DuckDBPyConnection, topics_path: str = TOPICS_PATH) -> int:
    """Populate activity_topics by keyword-matching lobbying_activities.description.

    Keywords match on word boundaries (regexp, case-insensitive), not raw substrings —
    "ai" must not fire inside "said"/"maintain", and "AI." / "AI," must still match, which
    the old LIKE-with-padded-spaces approach got wrong on both counts.

    Runs once at publish time (not per query) — the frontend just filters on the
    precomputed tags. Re-running is idempotent (clears and rebuilds).
    """
    topics = load_topics(topics_path)
    con.execute("DELETE FROM activity_topics")

    total_tagged = 0
    for topic_key, cfg in topics.items():
        codes = cfg.get("general_issue_codes") or []
        keywords = cfg["keywords"]

        # One combined alternation per topic: \b(kw1|kw2|...)\b, case-insensitive.
        pattern = r"\b(" + "|".join(re.escape(kw.strip()) for kw in keywords) + r")\b"

        code_clause = ""
        if codes:
            placeholders = ", ".join("?" for _ in codes)
            code_clause = f" AND general_issue_code IN ({placeholders})"

        sql = f"""
            INSERT INTO activity_topics (activity_id, topic_key)
            SELECT activity_id, ? FROM lobbying_activities
            WHERE description IS NOT NULL
              AND regexp_matches(description, ?, 'i') {code_clause}
        """
        params = [topic_key, pattern] + list(codes)

        con.execute(sql, params)
        total_tagged += con.execute(
            "SELECT count(*) FROM activity_topics WHERE topic_key = ?", [topic_key]
        ).fetchone()[0]

    return total_tagged


def build_org_first_seen(con: duckdb.DuckDBPyConnection) -> int:
    """Rebuild org_first_seen: earliest dt_posted per normalized client name.

    A filing counts as "first-time" only when its dt_posted equals its client's global
    first-seen date here — not merely the first filing_uuid for that client_id, since the
    same real org can pick up a new registrant/firm without being a new lobbying player.
    Cheap full recompute at this data volume (low millions of rows), matching the Canadian
    app's approach.
    """
    con.execute("DELETE FROM org_first_seen")
    con.execute("""
        INSERT INTO org_first_seen (name_norm, first_date, display_name, first_client_id)
        SELECT
            c.name_norm,
            min(f.dt_posted)::DATE AS first_date,
            arg_min(c.name, f.dt_posted) AS display_name,
            arg_min(c.client_id, f.dt_posted) AS first_client_id
        FROM filings f
        JOIN clients c ON c.client_id = f.client_id
        WHERE c.name_norm IS NOT NULL AND c.name_norm != ''
        GROUP BY c.name_norm
    """)
    return con.execute("SELECT count(*) FROM org_first_seen").fetchone()[0]


if __name__ == "__main__":
    assert norm_name("Acme Corp.") == "acme"
    assert norm_name("Acme Corporation") == "acme"
    assert norm_name("The Smith Group, LLC") == "the smith group"
    assert norm_name("O'Brien & Associates (2024)") == "o brien associates"
    print("norm_name self-checks passed")
