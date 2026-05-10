import type { NodeSpec } from "../architecture";

const LAYER_LABEL: Record<NodeSpec["layer"], string> = {
  upstream: "Upstream source",
  ingest: "Ingest layer",
  "storage-cold": "Cold storage",
  "storage-hot": "Live database",
  rules: "Rules repo",
  consumer: "Consumer",
};

export function DetailPanel({
  node,
  onClose,
}: {
  node: NodeSpec | null;
  onClose: () => void;
}) {
  if (!node) {
    return (
      <aside className="detail-panel detail-panel--empty">
        <h2>Click any node</h2>
        <p>
          The architecture is laid out left-to-right: upstream → ingest → storage → consumers.
          Click on a node to see what it owns, where it lives in the repo, and how it connects to
          its neighbors.
        </p>
        <p>
          Use the buttons on the left to switch between scenes. Drag to pan, scroll to zoom.
        </p>
      </aside>
    );
  }

  return (
    <aside className="detail-panel">
      <button className="detail-panel__close" onClick={onClose} aria-label="Close">
        ×
      </button>
      <div className="detail-panel__layer">{LAYER_LABEL[node.layer]}</div>
      <h2>{node.label.replace(/\n/g, " ")}</h2>
      <p className="detail-panel__summary">{node.summary}</p>
      <pre className="detail-panel__detail">{node.detail}</pre>
      {node.source && (
        <div className="detail-panel__source">
          <span>Source:</span> <code>{node.source}</code>
        </div>
      )}
    </aside>
  );
}
