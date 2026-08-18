import { Fragment } from "preact";

type SyntaxLanguage = "json" | "yaml";

interface Token {
  text: string;
  kind?: string;
}

export function SyntaxHighlight({
  value,
  language,
  class: className,
}: {
  value: string;
  language: SyntaxLanguage;
  class?: string;
}) {
  const tokens = language === "json" ? jsonTokens(value) : yamlTokens(value);
  return (
    <code class={["syntax-code", className].filter(Boolean).join(" ")}>
      {tokens.map((token, index) => (
        <Fragment key={index}>
          {token.kind === undefined ? (
            token.text
          ) : (
            <span class={`syntax-${token.kind}`}>{token.text}</span>
          )}
        </Fragment>
      ))}
    </code>
  );
}

function jsonTokens(value: string): Token[] {
  return tokenize(
    value,
    /"(?:\\.|[^"\\])*"\s*:|"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b/g,
    (text) => {
      if (text.startsWith('"'))
        return text.trimEnd().endsWith(":") ? "key" : "string";
      if (text === "true" || text === "false") return "boolean";
      if (text === "null") return "null";
      return "number";
    },
  );
}

function yamlTokens(value: string): Token[] {
  return tokenize(
    value,
    /#[^\n]*|^[ \t-]*[A-Za-z_][\w.-]*(?=\s*:)|"(?:\\.|[^"\\])*"|'(?:''|[^'])*'|\b(?:true|false|null|yes|no)\b|-?\d+(?:\.\d+)?/gim,
    (text) => {
      if (text.trimStart().startsWith("#")) return "comment";
      if (/^[ \t-]*[A-Za-z_][\w.-]*$/i.test(text)) return "key";
      if (text.startsWith('"') || text.startsWith("'")) return "string";
      if (/^(?:true|false|yes|no)$/i.test(text)) return "boolean";
      if (/^null$/i.test(text)) return "null";
      return "number";
    },
  );
}

function tokenize(
  value: string,
  pattern: RegExp,
  classify: (text: string) => string,
): Token[] {
  const tokens: Token[] = [];
  let offset = 0;
  for (const match of value.matchAll(pattern)) {
    const index = match.index ?? 0;
    if (index > offset) tokens.push({ text: value.slice(offset, index) });
    tokens.push({ text: match[0], kind: classify(match[0]) });
    offset = index + match[0].length;
  }
  if (offset < value.length) tokens.push({ text: value.slice(offset) });
  return tokens;
}
