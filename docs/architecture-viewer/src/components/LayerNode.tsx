import { Handle, Position } from "@xyflow/react";
import type { Layer, Repo } from "../architecture";

const REPO_LABEL: Record<Repo, string> = {
  "axiom-corpus": "axiom-corpus",
  "axiom-encode": "axiom-encode",
  "axiom-foundation.org": "axiom-foundation.org",
  "rules-us": "rules-us",
  "rules-us-state": "rules-us-{*}",
  "rules-non-us": "rules-uk · rules-ca",
  infrastructure: "Managed infra",
  external: "External source",
};

const LAYER_GLYPH: Record<Layer, string> = {
  upstream: "§",
  ingest: "→",
  "storage-cold": "□",
  "storage-hot": "■",
  rules: "¶",
  consumer: "◇",
};

export type LayerNodeData = {
  label: string;
  summary: string;
  layer: Layer;
  repo: Repo;
  selected: boolean;
  related: boolean;
  dimmed: boolean;
  [key: string]: unknown;
};

export function LayerNode({ data }: { data: LayerNodeData }) {
  const cls = [
    "axiom-node",
    `axiom-node--${data.layer}`,
    data.selected ? "axiom-node--selected" : "",
    data.related ? "axiom-node--related" : "",
    data.dimmed ? "axiom-node--dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cls}>
      <Handle type="target" position={Position.Left} className="axiom-node__handle" />
      <div className="axiom-node__eyebrow">
        <span className="axiom-node__glyph">{LAYER_GLYPH[data.layer]}</span>
        <span className="axiom-node__repo">{REPO_LABEL[data.repo]}</span>
      </div>
      <div className="axiom-node__title">{data.label}</div>
      <div className="axiom-node__summary">{data.summary}</div>
      <Handle type="source" position={Position.Right} className="axiom-node__handle" />
    </div>
  );
}
