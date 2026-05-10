import { useCallback, useMemo, useState } from "react";
import {
  Background,
  Controls,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
} from "@xyflow/react";

import { LayerNode } from "./components/LayerNode";
import type { LayerNodeData } from "./components/LayerNode";
import { LabeledEdge } from "./components/LabeledEdge";
import { DetailPanel } from "./components/DetailPanel";
import { SceneSwitcher } from "./components/SceneSwitcher";
import {
  LAYOUTS,
  NODES,
  REPOS,
  neighborsOf,
  type EdgeSpec,
  type NodeSpec,
} from "./architecture";

const NODE_TYPES = { layer: LayerNode };
const EDGE_TYPES = { labeled: LabeledEdge };

const EDGE_STYLES: Record<
  EdgeSpec["kind"],
  { stroke: string; strokeWidth: number; dash?: string }
> = {
  solid: { stroke: "#1c1917", strokeWidth: 2 },
  derived: { stroke: "#92400e", strokeWidth: 2, dash: "6 4" },
  read: { stroke: "#78716c", strokeWidth: 1.5, dash: "2 4" },
};

function toRfNodes(
  layoutNodes: { id: string; x: number; y: number }[],
  catalog: Map<string, NodeSpec>,
  selectedId: string | null,
  relatedIds: Set<string>,
): Node[] {
  const hasSelection = selectedId !== null;
  const out: Node[] = [];
  for (const entry of layoutNodes) {
    const spec = catalog.get(entry.id);
    if (!spec) continue;
    const isSelected = entry.id === selectedId;
    const isRelated = relatedIds.has(entry.id);
    const data: LayerNodeData = {
      label: spec.label,
      summary: spec.summary,
      layer: spec.layer,
      repo: spec.repo,
      selected: isSelected,
      related: isRelated,
      dimmed: hasSelection && !isSelected && !isRelated,
    };
    out.push({
      id: entry.id,
      type: "layer",
      position: { x: entry.x, y: entry.y },
      data,
    });
  }
  return out;
}

function toRfEdges(
  layoutEdges: EdgeSpec[],
  selectedId: string | null,
): Edge[] {
  return layoutEdges.map((edge, index) => {
    const style = EDGE_STYLES[edge.kind];
    const highlighted =
      selectedId !== null && (edge.from === selectedId || edge.to === selectedId);
    const dimmed = selectedId !== null && !highlighted;
    return {
      id: `${edge.from}-${edge.to}-${index}`,
      source: edge.from,
      target: edge.to,
      label: edge.label,
      data: { highlighted },
      style: {
        stroke: highlighted ? "#92400e" : style.stroke,
        strokeWidth: highlighted ? style.strokeWidth + 1 : style.strokeWidth,
        strokeDasharray: style.dash,
        opacity: dimmed ? 0.15 : 1,
        transition: "stroke 200ms ease, opacity 200ms ease",
      },
      type: "labeled",
      animated: edge.kind === "derived" && (highlighted || selectedId === null),
    };
  });
}

export function App() {
  const [activeLayoutId, setActiveLayoutId] = useState(LAYOUTS[0].id);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const catalog = useMemo(() => new Map(NODES.map((node) => [node.id, node])), []);

  const layout = useMemo(
    () => LAYOUTS.find((l) => l.id === activeLayoutId) ?? LAYOUTS[0],
    [activeLayoutId],
  );

  const neighbors = useMemo(
    () => (selectedId ? neighborsOf(selectedId, layout.edges) : null),
    [selectedId, layout.edges],
  );

  const relatedIds = useMemo(() => {
    if (!neighbors) return new Set<string>();
    const ids = new Set<string>();
    for (const n of neighbors.incoming) ids.add(n.node.id);
    for (const n of neighbors.outgoing) ids.add(n.node.id);
    return ids;
  }, [neighbors]);

  const rfNodes = useMemo(
    () => toRfNodes(layout.nodes, catalog, selectedId, relatedIds),
    [layout, catalog, selectedId, relatedIds],
  );
  const rfEdges = useMemo(
    () => toRfEdges(layout.edges, selectedId),
    [layout, selectedId],
  );

  const handleNodeClick = useCallback<NodeMouseHandler>((_, node) => {
    setSelectedId(node.id);
  }, []);

  const selectedNode = selectedId ? catalog.get(selectedId) ?? null : null;
  const detailOpen = selectedNode !== null;

  return (
    <div className={`layout ${detailOpen ? "layout--detail-open" : ""}`}>
      <SceneSwitcher
        layouts={LAYOUTS}
        activeId={activeLayoutId}
        onChange={(id) => {
          setActiveLayoutId(id);
          setSelectedId(null);
        }}
        repos={REPOS}
      />
      <main className="canvas">
        <header className="canvas__header">
          <div className="eyebrow">{layout.eyebrow}</div>
          <h1 className="heading-section">{layout.title}</h1>
          <p>{layout.description}</p>
        </header>
        <div className="canvas__flow">
          <ReactFlow
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={NODE_TYPES}
            edgeTypes={EDGE_TYPES}
            onNodeClick={handleNodeClick}
            onPaneClick={() => setSelectedId(null)}
            fitView
            fitViewOptions={{ padding: 0.18 }}
            minZoom={0.25}
            maxZoom={1.6}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={24} size={1} color="#e7e5e4" />
            <Controls showInteractive={false} />
          </ReactFlow>
        </div>
      </main>
      {selectedNode && (
        <DetailPanel
          node={selectedNode}
          incoming={neighbors?.incoming ?? []}
          outgoing={neighbors?.outgoing ?? []}
          onSelectNode={setSelectedId}
          onClose={() => setSelectedId(null)}
        />
      )}
    </div>
  );
}
