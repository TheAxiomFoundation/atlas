import { Handle, Position } from "@xyflow/react";
import type { Layer } from "../architecture";

const LAYER_STYLES: Record<Layer, { background: string; border: string; accent: string }> = {
  upstream: {
    background: "#fef3c7",
    border: "#d97706",
    accent: "#92400e",
  },
  ingest: {
    background: "#dbeafe",
    border: "#1d4ed8",
    accent: "#1e3a8a",
  },
  "storage-cold": {
    background: "#e5e7eb",
    border: "#4b5563",
    accent: "#1f2937",
  },
  "storage-hot": {
    background: "#dcfce7",
    border: "#15803d",
    accent: "#14532d",
  },
  rules: {
    background: "#ede9fe",
    border: "#6d28d9",
    accent: "#4c1d95",
  },
  consumer: {
    background: "#fce7f3",
    border: "#be185d",
    accent: "#831843",
  },
};

export type LayerNodeData = {
  label: string;
  summary: string;
  layer: Layer;
  selected: boolean;
  [key: string]: unknown;
};

export function LayerNode({ data }: { data: LayerNodeData }) {
  const palette = LAYER_STYLES[data.layer];
  return (
    <div
      style={{
        background: palette.background,
        border: `2px solid ${palette.border}`,
        borderRadius: 8,
        padding: "10px 14px",
        minWidth: 180,
        maxWidth: 220,
        boxShadow: data.selected ? `0 0 0 3px ${palette.border}55` : "0 1px 2px rgba(0,0,0,0.08)",
        fontFamily: "system-ui, -apple-system, sans-serif",
      }}
    >
      <Handle type="target" position={Position.Left} style={{ background: palette.border }} />
      <div
        style={{
          fontWeight: 600,
          fontSize: 13,
          color: palette.accent,
          marginBottom: 4,
          whiteSpace: "pre-wrap",
        }}
      >
        {data.label}
      </div>
      <div style={{ fontSize: 11, color: "#374151", lineHeight: 1.35 }}>{data.summary}</div>
      <Handle type="source" position={Position.Right} style={{ background: palette.border }} />
    </div>
  );
}
