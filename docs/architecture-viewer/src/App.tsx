import { useCallback, useMemo, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
} from "@xyflow/react";

import { LayerNode } from "./components/LayerNode";
import type { LayerNodeData } from "./components/LayerNode";
import { DetailPanel } from "./components/DetailPanel";
import { SceneSwitcher } from "./components/SceneSwitcher";
import {
  EDGES as _EDGES,
  LAYOUTS,
  NODES,
  type EdgeSpec,
  type NodeSpec,
} from "./architecture";

void _EDGES;

const NODE_TYPES = { layer: LayerNode };

const EDGE_STYLES: Record<EdgeSpec["kind"], { stroke: string; strokeWidth: number; dash?: string }> = {
  solid: { stroke: "#1f2937", strokeWidth: 2 },
  derived: { stroke: "#6d28d9", strokeWidth: 2, dash: "4 4" },
  read: { stroke: "#be185d", strokeWidth: 2, dash: "2 2" },
};

function toRfNodes(
  layoutNodes: { id: string; x: number; y: number }[],
  catalog: Map<string, NodeSpec>,
  selectedId: string | null,
): Node[] {
  const out: Node[] = [];
  for (const entry of layoutNodes) {
    const spec = catalog.get(entry.id);
    if (!spec) continue;
    const data: LayerNodeData = {
      label: spec.label,
      summary: spec.summary,
      layer: spec.layer,
      selected: selectedId === entry.id,
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

function toRfEdges(layoutEdges: EdgeSpec[]): Edge[] {
  return layoutEdges.map((edge, index) => {
    const style = EDGE_STYLES[edge.kind];
    return {
      id: `${edge.from}-${edge.to}-${index}`,
      source: edge.from,
      target: edge.to,
      label: edge.label,
      labelStyle: { fontSize: 11, fill: "#374151", fontFamily: "system-ui" },
      labelBgStyle: { fill: "#ffffff", fillOpacity: 0.9 },
      labelBgPadding: [4, 2],
      style: {
        stroke: style.stroke,
        strokeWidth: style.strokeWidth,
        strokeDasharray: style.dash,
      },
      type: "smoothstep",
      animated: edge.kind === "derived",
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

  const rfNodes = useMemo(
    () => toRfNodes(layout.nodes, catalog, selectedId),
    [layout, catalog, selectedId],
  );
  const rfEdges = useMemo(() => toRfEdges(layout.edges), [layout]);

  const handleNodeClick = useCallback<NodeMouseHandler>((_, node) => {
    setSelectedId(node.id);
  }, []);

  const selectedNode = selectedId ? catalog.get(selectedId) ?? null : null;

  return (
    <div className="layout">
      <SceneSwitcher
        layouts={LAYOUTS}
        activeId={activeLayoutId}
        onChange={(id) => {
          setActiveLayoutId(id);
          setSelectedId(null);
        }}
      />
      <main className="canvas">
        <header className="canvas__header">
          <h1>{layout.title}</h1>
          <p>{layout.description}</p>
        </header>
        <div className="canvas__flow">
          <ReactFlow
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={NODE_TYPES}
            onNodeClick={handleNodeClick}
            onPaneClick={() => setSelectedId(null)}
            fitView
            fitViewOptions={{ padding: 0.15 }}
            minZoom={0.3}
            maxZoom={1.6}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} size={1} color="#e5e7eb" />
            <Controls showInteractive={false} />
            <MiniMap pannable zoomable nodeColor="#cbd5e1" maskColor="rgba(15, 23, 42, 0.04)" />
          </ReactFlow>
        </div>
      </main>
      <DetailPanel node={selectedNode} onClose={() => setSelectedId(null)} />
    </div>
  );
}
