import type { ComponentChildren } from "preact";
import { useLayoutEffect, useRef, useState } from "preact/hooks";
import type { TableRule } from "../../generated/apiContract";
import { useTableCatalog } from "../../schema/tableCatalog";
import { Button } from "../../ui/Button";
import { FormField } from "../../ui/FormField";
import { AvailableTablesDialog } from "./AvailableTablesDialog";
import { TablePatternInput } from "./TablePatternInput";
import { exactPattern } from "./model";
import { useTableNamespace } from "./naming";

const HELP = "Default: glob / wildcard, where * matches any number of characters and ? one character. The .* button enables regex independently for each field.";

/** One visual and interaction contract for source rules and transform scopes. */
export function TableRuleFields({ id, rule, labelSuffix, disabled, excludeExpanded, onExcludeExpanded,
  onChange, includeHelp, excludeHelp, confirmed = false, invalid = false, compact = false, trailing, onUse }: {
  id: string; rule: TableRule; labelSuffix: string; disabled: boolean;
  excludeExpanded: boolean; onExcludeExpanded: (expanded: boolean) => void;
  onChange: (patch: Partial<TableRule>) => void;
  includeHelp: string; excludeHelp: string;
  confirmed?: boolean; invalid?: boolean; compact?: boolean;
  trailing?: ComponentChildren; onUse?: (() => void) | undefined;
}) {
  const catalog = useTableCatalog();
  const namespace = useTableNamespace();
  const [browse, setBrowse] = useState(false);
  const focusField = useRef<string>();
  const excludeOpen = !!rule.exclude || excludeExpanded;
  useLayoutEffect(() => { if (!catalog || disabled) setBrowse(false); }, [catalog, disabled]);
  useLayoutEffect(() => {
    if (focusField.current === undefined) return;
    document.getElementById(focusField.current)?.focus({ preventScroll: true });
    focusField.current = undefined;
  });
  const field = (kind: "include" | "exclude") => {
    const title = kind === "include" ? "Include" : "Exclude";
    return <FormField label={title} optional={false} controlId={`${id}-${kind}`}
      description={`${HELP} Use ${namespace}.table or ${namespace}.*. ${kind === "include" ? includeHelp : excludeHelp}`}>
      <TablePatternInput id={`${id}-${kind}`} label={`${title} ${labelSuffix}`} value={rule[kind] ?? ""}
        mode={rule[`${kind}_mode`] ?? "glob"} disabled={disabled} required={kind === "include"}
        invalid={kind === "include" && invalid} confirmed={kind === "include" ? confirmed : undefined}
        onBrowse={kind === "include" ? () => setBrowse(true) : undefined}
        onChange={value => {
          if (kind === "exclude") onExcludeExpanded(true);
          onChange({ [kind]: value });
        }} onModeChange={mode => onChange({ [`${kind}_mode`]: mode })} />
    </FormField>;
  };
  return <>
    <div class={`table-rule-patterns${excludeOpen ? " table-rule-with-exclude" : ""}${compact ? " table-rule-compact" : ""}${trailing ? "" : " table-rule-without-action"}`}>
      {field("include")}
      {excludeOpen ? <div class="table-exclude-field">
        {field("exclude")}
        <Button variant="plain" class="table-exclude-hide" aria-label={`Hide Exclude for ${labelSuffix}`}
          title={rule.exclude ? "Clear Exclude to hide it" : "Hide empty Exclude"}
          disabled={disabled || !!rule.exclude} aria-expanded="true" aria-controls={`${id}-exclude`}
          onClick={() => {
            focusField.current = `${id}-add-exclude`;
            onExcludeExpanded(false);
          }}>Hide</Button>
      </div> : <Button id={`${id}-add-exclude`} variant="plain" class="table-exclude-add"
        aria-label={`Add Exclude for ${labelSuffix}`} aria-expanded="false" disabled={disabled}
        onClick={() => {
          focusField.current = `${id}-exclude`;
          onExcludeExpanded(true);
        }}><span aria-hidden="true">+</span> Exclude</Button>}
      {trailing}
    </div>
    {browse && catalog && !disabled && <AvailableTablesDialog catalog={catalog} onClose={() => setBrowse(false)}
      onUse={table => { onUse?.(); onChange({ include: exactPattern(table, rule.include_mode ?? "glob") }); }} />}
  </>;
}
