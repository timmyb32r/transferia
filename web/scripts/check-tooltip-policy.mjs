import ts from "typescript";

export function checkTooltipPolicy(path, source) {
  const violations = [];
  // Native titles remain the default. Only the shared CopyButton owns the
  // approved stateful Copy/Copied overlay. Truncated table patterns have the
  // explicitly requested immediate full-value overlay, never a native title.
  if (/data-tooltip|content\s*:\s*attr\(\s*title/.test(source))
    violations.push(`${path}: use native title, not a second tooltip renderer`);
  if (!path.endsWith(".tsx")) return violations;
  const tree = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const copyComponent = path === "ui/CopyButton.tsx";
  const tablePatternComponent = path === "features/tableSelection/TablePatternInput.tsx";
  const visit = (node) => {
    if (ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) {
      const attributes = node.attributes.properties.filter(ts.isJsxAttribute);
      const attribute = (name) => attributes.find((item) => item.name.getText(tree) === name);
      if (copyComponent && attribute("title"))
        violations.push(`${path}: CopyButton must not combine its overlay with a native title`);
      const role = attribute("role")?.initializer;
      if (role && ts.isStringLiteral(role) && role.text === "tooltip") {
        const className = attribute("class")?.initializer;
        const hidden = className && ts.isStringLiteral(className) && className.text === "visually-hidden";
        const copyClass = className && (ts.isStringLiteral(className) && className.text === "copy-tooltip"
          || ts.isJsxExpression(className) && className.expression && ts.isTemplateExpression(className.expression)
            && className.expression.head.text === "copy-tooltip");
        const tableClass = className && ts.isJsxExpression(className) && className.expression
          && ts.isTemplateExpression(className.expression) && className.expression.head.text === "table-pattern-tooltip";
        if (!hidden && !(copyComponent && copyClass) && !(tablePatternComponent && tableClass))
          violations.push(`${path}: tooltip descriptions must use class="visually-hidden"; visual overlays belong only to shared CopyButton and TablePatternInput`);
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(tree);
  return violations;
}
