import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from "@xyflow/react";

// HTML-overlay edge label. We render the path via BaseEdge (SVG) and the
// label via EdgeLabelRenderer (HTML), guaranteeing the chip sits above the
// SVG line in every configuration — SVG label rects could be overdrawn by
// other edge segments at smoothstep corners.
export function LabeledEdge(props: EdgeProps) {
  const {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style,
    label,
    markerEnd,
    data,
  } = props;

  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const highlighted = (data as { highlighted?: boolean } | undefined)?.highlighted ?? false;

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={style} markerEnd={markerEnd} />
      {label && (
        <EdgeLabelRenderer>
          <div
            className={`edge-label ${highlighted ? "edge-label--highlighted" : ""}`}
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}
