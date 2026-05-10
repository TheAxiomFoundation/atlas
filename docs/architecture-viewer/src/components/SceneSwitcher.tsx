import type { Layout, RepoSpec } from "../architecture";

export function SceneSwitcher({
  layouts,
  activeId,
  onChange,
  repos,
}: {
  layouts: Layout[];
  activeId: string;
  onChange: (id: string) => void;
  repos: RepoSpec[];
}) {
  return (
    <nav className="scene-switcher">
      <div className="scene-switcher__wordmark">
        <span className="glyph-axiom">∀</span>
        <span className="scene-switcher__wordmark-text">axiom-corpus</span>
      </div>
      <div className="scene-switcher__subtitle">Architecture</div>

      <ul className="scene-switcher__scenes">
        {layouts.map((layout) => {
          const active = layout.id === activeId;
          return (
            <li key={layout.id}>
              <button
                className={`scene-switcher__btn ${
                  active ? "scene-switcher__btn--active" : ""
                }`}
                onClick={() => onChange(layout.id)}
              >
                <span className="scene-switcher__btn-eyebrow">{layout.eyebrow}</span>
                <span className="scene-switcher__btn-title">{layout.title}</span>
                <span className="scene-switcher__btn-description">{layout.description}</span>
              </button>
            </li>
          );
        })}
      </ul>

      <div className="scene-switcher__legend">
        <div className="scene-switcher__legend-title">Edges</div>
        <ul>
          <li>
            <span className="edge-swatch edge-swatch--solid" /> source-of-truth data flow
          </li>
          <li>
            <span className="edge-swatch edge-swatch--derived" /> derived / rebuildable
          </li>
          <li>
            <span className="edge-swatch edge-swatch--read" /> read-only consumer
          </li>
        </ul>

        <div className="scene-switcher__legend-title">Repositories</div>
        <ul className="scene-switcher__repos">
          {repos.map((r) => (
            <li key={r.id}>
              <span className="scene-switcher__repo-name">{r.label}</span>
              <span className="scene-switcher__repo-desc">{r.description}</span>
            </li>
          ))}
        </ul>
      </div>
    </nav>
  );
}
