export function checkSchemaInspectorLayout(css) {
  const violations = [];
  const inspector = css.match(/\.schema-inspector\s*\{([^}]*)\}/)?.[1] ?? "";
  const table = css.match(/\.schema-inspector-table\s*\{([^}]*)\}/)?.[1] ?? "";
  // The fixed shell owns resize geometry; only the table scrolls so updates
  // cannot move the toolbar or steal width from it when a scrollbar appears.
  if (!inspector.includes("overflow: hidden"))
    violations.push("style.css: schema inspector must keep its toolbar outside the scrolling table");
  if (!/overflow:\s*(auto|scroll)\s*;/.test(table))
    violations.push("style.css: schema inspector table must scroll independently");
  if (!/scrollbar-gutter:\s*stable(?:\s+both-edges)?\s*;/.test(table))
    violations.push("style.css: schema inspector table must reserve a stable scrollbar gutter");
  if (!inspector.includes("max-height: calc(100dvh - 48px)"))
    violations.push("style.css: schema inspector must keep its resize handle inside the initial viewport");
  return violations;
}
