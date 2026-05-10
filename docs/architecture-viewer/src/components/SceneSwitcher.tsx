import type { Layout } from "../architecture";

export function SceneSwitcher({
  layouts,
  activeId,
  onChange,
}: {
  layouts: Layout[];
  activeId: string;
  onChange: (id: string) => void;
}) {
  return (
    <nav className="scene-switcher">
      <div className="scene-switcher__title">axiom-corpus</div>
      <div className="scene-switcher__subtitle">architecture viewer</div>
      <ul>
        {layouts.map((layout) => (
          <li key={layout.id}>
            <button
              className={`scene-switcher__btn ${
                layout.id === activeId ? "scene-switcher__btn--active" : ""
              }`}
              onClick={() => onChange(layout.id)}
            >
              <span className="scene-switcher__btn-title">{layout.title}</span>
              <span className="scene-switcher__btn-description">{layout.description}</span>
            </button>
          </li>
        ))}
      </ul>
      <div className="scene-switcher__legend">
        <div className="scene-switcher__legend-title">Layers</div>
        <ul>
          <li><span className="dot dot--upstream" /> Upstream</li>
          <li><span className="dot dot--ingest" /> Ingest</li>
          <li><span className="dot dot--cold" /> Cold storage</li>
          <li><span className="dot dot--hot" /> Live database</li>
          <li><span className="dot dot--rules" /> Rules repos</li>
          <li><span className="dot dot--consumer" /> Consumers</li>
        </ul>
        <div className="scene-switcher__legend-title">Edges</div>
        <ul>
          <li>
            <svg width="32" height="6">
              <line x1="0" y1="3" x2="32" y2="3" stroke="#1f2937" strokeWidth="2" />
            </svg>{" "}
            data flow
          </li>
          <li>
            <svg width="32" height="6">
              <line x1="0" y1="3" x2="32" y2="3" stroke="#6d28d9" strokeWidth="2" strokeDasharray="4 4" />
            </svg>{" "}
            derived
          </li>
          <li>
            <svg width="32" height="6">
              <line x1="0" y1="3" x2="32" y2="3" stroke="#be185d" strokeWidth="2" strokeDasharray="2 2" />
            </svg>{" "}
            read-only consumer
          </li>
        </ul>
      </div>
    </nav>
  );
}
