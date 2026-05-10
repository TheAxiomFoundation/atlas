import type { EdgeSpec, NodeSpec, Repo } from "../architecture";
import { REPOS } from "../architecture";

const REPO_INFO: Record<Repo, { label: string; description: string }> = Object.fromEntries(
  REPOS.map((r) => [r.id, { label: r.label, description: r.description }]),
) as Record<Repo, { label: string; description: string }>;

const EDGE_KIND_LABEL: Record<EdgeSpec["kind"], string> = {
  solid: "writes",
  derived: "derives",
  read: "reads",
};

export function DetailPanel({
  node,
  incoming,
  outgoing,
  onSelectNode,
  onClose,
}: {
  node: NodeSpec;
  incoming: { node: NodeSpec; edge: EdgeSpec }[];
  outgoing: { node: NodeSpec; edge: EdgeSpec }[];
  onSelectNode: (id: string) => void;
  onClose: () => void;
}) {
  const repo = REPO_INFO[node.repo];

  return (
    <aside className="detail-panel">
      <button className="detail-panel__close" onClick={onClose} aria-label="Close">
        ×
      </button>

      <div className="kicker">
        <span className="kicker-mark">§</span> {repo.label}
      </div>
      <h2 className="detail-panel__h">{node.label.replace(/\n/g, " ")}</h2>
      <p className="detail-panel__summary">{node.summary}</p>

      <section className="detail-panel__section">
        <div className="detail-panel__section-label">Detail</div>
        <pre className="detail-panel__detail">{node.detail}</pre>
      </section>

      <section className="detail-panel__section">
        <div className="detail-panel__section-label">Repository</div>
        <div className="detail-panel__repo">
          <div className="detail-panel__repo-name">{repo.label}</div>
          <div className="detail-panel__repo-desc">{repo.description}</div>
        </div>
        {node.source && (
          <div className="detail-panel__source">
            <span>Source path:</span> <code>{node.source}</code>
          </div>
        )}
      </section>

      <section className="detail-panel__section">
        <div className="detail-panel__section-label">Receives from</div>
        {incoming.length === 0 ? (
          <p className="detail-panel__empty-list">No upstream dependencies.</p>
        ) : (
          <ul className="detail-panel__edges">
            {incoming.map(({ node: src, edge }, i) => (
              <li key={i}>
                <button
                  className={`detail-panel__edge detail-panel__edge--${edge.kind}`}
                  onClick={() => onSelectNode(src.id)}
                >
                  <span className="detail-panel__edge-verb">
                    {EDGE_KIND_LABEL[edge.kind]}
                  </span>
                  <span className="detail-panel__edge-target">{src.label.replace(/\n/g, " ")}</span>
                  {edge.label && (
                    <span className="detail-panel__edge-label">— {edge.label}</span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="detail-panel__section">
        <div className="detail-panel__section-label">Sends to</div>
        {outgoing.length === 0 ? (
          <p className="detail-panel__empty-list">Terminal — nothing reads from it.</p>
        ) : (
          <ul className="detail-panel__edges">
            {outgoing.map(({ node: dst, edge }, i) => (
              <li key={i}>
                <button
                  className={`detail-panel__edge detail-panel__edge--${edge.kind}`}
                  onClick={() => onSelectNode(dst.id)}
                >
                  <span className="detail-panel__edge-verb">
                    {EDGE_KIND_LABEL[edge.kind]}
                  </span>
                  <span className="detail-panel__edge-target">{dst.label.replace(/\n/g, " ")}</span>
                  {edge.label && (
                    <span className="detail-panel__edge-label">— {edge.label}</span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>
    </aside>
  );
}
