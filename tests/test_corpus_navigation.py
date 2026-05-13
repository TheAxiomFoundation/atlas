"""Tests for the precomputed `corpus.navigation_nodes` builder."""

from __future__ import annotations

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.navigation import (
    build_navigation_nodes,
    deterministic_navigation_id,
    group_nodes_by_scope,
)
from axiom_corpus.corpus.supabase import deterministic_provision_id


def _record(
    citation_path: str,
    *,
    parent_citation_path: str | None = None,
    heading: str | None = None,
    ordinal: int | None = None,
    has_rulespec: bool | None = None,
    jurisdiction: str = "us-co",
    document_class: str = "statute",
    version: str | None = None,
    metadata: dict | None = None,
) -> ProvisionRecord:
    return ProvisionRecord(
        jurisdiction=jurisdiction,
        document_class=document_class,
        citation_path=citation_path,
        parent_citation_path=parent_citation_path,
        heading=heading,
        ordinal=ordinal,
        has_rulespec=has_rulespec,
        version=version,
        metadata=metadata,
    )


def test_parent_child_navigation_uses_explicit_parent_citation_path():
    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/title-39"),
            _record(
                "us-co/statute/title-39/article-22",
                parent_citation_path="us-co/statute/title-39",
                heading="Income Tax",
            ),
            _record(
                "us-co/statute/title-39/article-22/part-1",
                parent_citation_path="us-co/statute/title-39/article-22",
            ),
        ]
    )

    by_path = {node.path: node for node in nodes}
    title = by_path["us-co/statute/title-39"]
    article = by_path["us-co/statute/title-39/article-22"]
    part = by_path["us-co/statute/title-39/article-22/part-1"]

    assert title.parent_path is None
    assert article.parent_path == "us-co/statute/title-39"
    assert part.parent_path == "us-co/statute/title-39/article-22"
    assert title.depth == 0 and article.depth == 1 and part.depth == 2
    assert article.label == "Income Tax"
    assert part.label == "part-1"  # falls back to segment when no heading


def test_navigation_uses_versioned_provision_id_for_local_jsonl():
    nodes = build_navigation_nodes([
        _record("us-co/statute/title-39", version="2026-05-13"),
    ])

    assert nodes[0].provision_id == deterministic_provision_id(
        "us-co/statute/title-39",
        "2026-05-13",
    )


def test_citation_path_hierarchy_links_to_nearest_existing_ancestor():
    # No `parent_citation_path` set; ancestors should be inferred from prefix.
    nodes = build_navigation_nodes(
        [
            _record("us/statute/26"),
            _record("us/statute/26/A/1/A/32", heading="Earned income"),
            _record("us/statute/26/A/1/A/24"),
        ]
    )
    by_path = {node.path: node for node in nodes}

    assert by_path["us/statute/26"].parent_path is None
    assert by_path["us/statute/26/A/1/A/32"].parent_path == "us/statute/26"
    assert by_path["us/statute/26/A/1/A/24"].parent_path == "us/statute/26"
    assert by_path["us/statute/26"].child_count == 2
    assert by_path["us/statute/26"].has_children is True


def test_does_not_invent_intermediate_nodes_for_missing_prefixes():
    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/title-39/article-22/part-1/section-101"),
        ]
    )
    paths = [node.path for node in nodes]
    # No synthetic ancestors get fabricated.
    assert paths == ["us-co/statute/title-39/article-22/part-1/section-101"]
    assert nodes[0].parent_path is None
    assert nodes[0].depth == 0


def test_missing_explicit_parent_does_not_create_hidden_orphan():
    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/title-39"),
            _record(
                "us-co/statute/title-39/article-22/part-1",
                parent_citation_path="us-co/statute/title-39/article-22",
            ),
        ]
    )
    by_path = {node.path: node for node in nodes}

    assert (
        by_path["us-co/statute/title-39/article-22/part-1"].parent_path == "us-co/statute/title-39"
    )
    assert by_path["us-co/statute/title-39"].child_count == 1


def test_child_count_and_has_children_are_consistent():
    nodes = build_navigation_nodes(
        [
            _record("a/b/root"),
            _record("a/b/root/x", parent_citation_path="a/b/root"),
            _record("a/b/root/y", parent_citation_path="a/b/root"),
            _record("a/b/root/y/z", parent_citation_path="a/b/root/y"),
        ]
    )
    by_path = {node.path: node for node in nodes}
    assert by_path["a/b/root"].child_count == 2
    assert by_path["a/b/root/y"].child_count == 1
    assert by_path["a/b/root/x"].child_count == 0
    assert by_path["a/b/root/x"].has_children is False
    assert by_path["a/b/root/y"].has_children is True


def test_sort_key_orders_segments_by_natural_order():
    nodes = build_navigation_nodes(
        [
            _record("a/2", parent_citation_path="a"),
            _record("a/10", parent_citation_path="a"),
            _record("a"),
            _record("a/1", parent_citation_path="a"),
        ]
    )
    children_of_a = [n for n in nodes if n.parent_path == "a"]
    children_of_a.sort(key=lambda n: n.sort_key)
    assert [n.segment for n in children_of_a] == ["1", "2", "10"]


def test_sort_key_uses_explicit_ordinal_when_provided():
    nodes = build_navigation_nodes(
        [
            _record("scope/root"),
            _record("scope/root/a", parent_citation_path="scope/root", ordinal=200),
            _record("scope/root/b", parent_citation_path="scope/root", ordinal=10),
        ]
    )
    children = sorted(
        (n for n in nodes if n.parent_path == "scope/root"),
        key=lambda n: n.sort_key,
    )
    assert [n.segment for n in children] == ["b", "a"]


def test_has_rulespec_and_encoded_descendant_count_propagate():
    nodes = build_navigation_nodes(
        [
            _record("scope/root"),
            _record("scope/root/a", parent_citation_path="scope/root"),
            _record(
                "scope/root/a/encoded",
                parent_citation_path="scope/root/a",
                has_rulespec=True,
            ),
            _record(
                "scope/root/b",
                parent_citation_path="scope/root",
                has_rulespec=True,
            ),
        ]
    )
    by_path = {node.path: node for node in nodes}
    # has_rulespec is taken straight from the provision.
    assert by_path["scope/root/a/encoded"].has_rulespec is True
    assert by_path["scope/root/a"].has_rulespec is False
    # Counts propagate bottom-up (excluding self where has_rulespec is False).
    assert by_path["scope/root/a/encoded"].encoded_descendant_count == 0
    assert by_path["scope/root/a"].encoded_descendant_count == 1
    assert by_path["scope/root/b"].encoded_descendant_count == 0
    assert by_path["scope/root"].encoded_descendant_count == 2


def test_node_id_is_deterministic_and_unique():
    nodes_first = build_navigation_nodes(
        [
            _record("alpha"),
            _record("alpha/beta", parent_citation_path="alpha"),
        ]
    )
    nodes_second = build_navigation_nodes(
        [
            _record("alpha/beta", parent_citation_path="alpha"),
            _record("alpha"),
        ]
    )
    assert [n.id for n in nodes_first] == [n.id for n in nodes_second]
    assert nodes_first[0].id == deterministic_navigation_id(nodes_first[0].path)
    assert len({n.id for n in nodes_first}) == len(nodes_first)


def test_node_id_includes_version_when_present():
    legacy = build_navigation_nodes([_record("alpha")])[0]
    versioned = build_navigation_nodes([_record("alpha", version="2026-05-13")])[0]

    assert versioned.id == deterministic_navigation_id("alpha", "2026-05-13")
    assert versioned.id != legacy.id
    assert versioned.version == "2026-05-13"


def test_group_nodes_by_scope_partitions_jurisdictions():
    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/x", jurisdiction="us-co", document_class="statute"),
            _record("us/regulation/7", jurisdiction="us", document_class="regulation"),
            _record(
                "us/regulation/7/273",
                jurisdiction="us",
                document_class="regulation",
                parent_citation_path="us/regulation/7",
            ),
        ]
    )
    grouped = group_nodes_by_scope(nodes)
    assert set(grouped.keys()) == {("us-co", "statute", None), ("us", "regulation", None)}
    assert len(grouped[("us", "regulation", None)]) == 2


def test_filter_by_jurisdiction_and_doc_type_excludes_other_scopes():
    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/x", jurisdiction="us-co", document_class="statute"),
            _record("us/regulation/7", jurisdiction="us", document_class="regulation"),
        ],
        jurisdiction="us-co",
        document_class="statute",
    )
    assert [n.path for n in nodes] == ["us-co/statute/x"]


def test_status_pulled_from_metadata_when_present():
    nodes = build_navigation_nodes(
        [
            _record("scope/x", metadata={"status": "current"}),
            _record("scope/y"),
        ]
    )
    by_path = {n.path: n for n in nodes}
    assert by_path["scope/x"].status == "current"
    assert by_path["scope/y"].status is None


def test_empty_input_returns_empty_tuple():
    assert build_navigation_nodes([]) == ()


def test_self_parent_is_promoted_to_root():
    nodes = build_navigation_nodes([_record("a", parent_citation_path="a")])
    assert len(nodes) == 1
    assert nodes[0].parent_path is None
    assert nodes[0].depth == 0


def test_two_cycle_is_broken_into_a_root_and_a_child():
    nodes = build_navigation_nodes(
        [
            _record("a", parent_citation_path="b"),
            _record("b", parent_citation_path="a"),
        ]
    )
    by_path = {n.path: n for n in nodes}
    # Exactly one of the two becomes a root; the other points at it.
    parents = {n.path: n.parent_path for n in nodes}
    roots = [p for p, parent in parents.items() if parent is None]
    assert len(roots) == 1
    other = "b" if roots[0] == "a" else "a"
    assert parents[other] == roots[0]
    assert by_path[roots[0]].depth == 0
    assert by_path[other].depth == 1


def test_three_cycle_is_broken_at_one_node():
    nodes = build_navigation_nodes(
        [
            _record("a", parent_citation_path="b"),
            _record("b", parent_citation_path="c"),
            _record("c", parent_citation_path="a"),
        ]
    )
    parents = {n.path: n.parent_path for n in nodes}
    roots = [p for p, parent in parents.items() if parent is None]
    assert len(roots) == 1
    # The remaining two nodes form a depth-1 / depth-2 chain reachable from the root.
    depths = {n.path: n.depth for n in nodes}
    assert sorted(depths.values()) == [0, 1, 2]


def test_cycle_breaking_keeps_non_cycle_descendants_attached_and_deterministic():
    records = [
        _record("x", parent_citation_path="a"),
        _record("a", parent_citation_path="b"),
        _record("b", parent_citation_path="a"),
    ]

    first = [n.to_supabase_row() for n in build_navigation_nodes(records)]
    second = [n.to_supabase_row() for n in build_navigation_nodes(reversed(records))]
    parents = {row["path"]: row["parent_path"] for row in first}
    depths = {row["path"]: row["depth"] for row in first}

    assert first == second
    assert parents == {"a": None, "b": "a", "x": "a"}
    assert depths == {"a": 0, "b": 1, "x": 1}


def test_filter_strands_child_whose_parent_is_in_another_scope():
    nodes = build_navigation_nodes(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/26",
            ),
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="statute",
                citation_path="us-co/statute/title-39",
                parent_citation_path="us/statute/26",
            ),
        ],
        jurisdiction="us-co",
    )
    assert [n.path for n in nodes] == ["us-co/statute/title-39"]
    assert nodes[0].parent_path is None


def test_ordinal_zero_sorts_before_unordained_records():
    nodes = build_navigation_nodes(
        [
            _record("p"),
            _record("p/a", parent_citation_path="p", ordinal=0),
            _record("p/b", parent_citation_path="p"),
        ]
    )
    children = sorted((n for n in nodes if n.parent_path == "p"), key=lambda n: n.sort_key)
    assert [n.segment for n in children] == ["a", "b"]


def test_has_rulespec_none_is_treated_as_false():
    nodes = build_navigation_nodes(
        [
            _record("root"),
            _record("root/leaf", parent_citation_path="root", has_rulespec=None),
        ]
    )
    by_path = {n.path: n for n in nodes}
    assert by_path["root/leaf"].has_rulespec is False
    assert by_path["root"].encoded_descendant_count == 0


def test_encoded_paths_set_has_rulespec_even_when_provision_says_false():
    nodes = build_navigation_nodes(
        [
            _record("us/statute/26"),
            _record("us/statute/26/3111", parent_citation_path="us/statute/26"),
            _record("us/statute/26/3111/a", parent_citation_path="us/statute/26/3111"),
        ],
        encoded_paths={"us/statute/26/3111/a"},
    )
    by_path = {n.path: n for n in nodes}

    assert by_path["us/statute/26/3111/a"].has_rulespec is True
    assert by_path["us/statute/26/3111"].has_rulespec is False
    # Direct encoded leaf has no encoded descendants of its own.
    assert by_path["us/statute/26/3111/a"].encoded_descendant_count == 0
    # Ancestors light up so encoded-only browsing is reachable from the top.
    assert by_path["us/statute/26/3111"].encoded_descendant_count == 1
    assert by_path["us/statute/26"].encoded_descendant_count == 1


def test_encoded_paths_for_paths_not_in_dataset_are_silently_ignored():
    nodes = build_navigation_nodes(
        [_record("us/statute/26")],
        encoded_paths={"us/statute/99/9999"},
    )
    assert [n.path for n in nodes] == ["us/statute/26"]
    assert nodes[0].has_rulespec is False
    assert nodes[0].encoded_descendant_count == 0


def test_encoded_paths_or_with_existing_provision_has_rulespec():
    nodes = build_navigation_nodes(
        [
            _record("a"),
            _record("a/b", parent_citation_path="a", has_rulespec=True),
            _record("a/c", parent_citation_path="a"),
        ],
        encoded_paths={"a/c"},
    )
    by_path = {n.path: n for n in nodes}
    assert by_path["a/b"].has_rulespec is True
    assert by_path["a/c"].has_rulespec is True
    assert by_path["a"].encoded_descendant_count == 2


def test_repeated_builds_emit_identical_rows():
    records = [
        _record("alpha"),
        _record("alpha/beta", parent_citation_path="alpha"),
        _record("alpha/beta/gamma", parent_citation_path="alpha/beta"),
    ]
    first = [n.to_supabase_row() for n in build_navigation_nodes(records)]
    second = [n.to_supabase_row() for n in build_navigation_nodes(reversed(records))]
    assert first == second
