import { Fragment } from "preact";

type SyntaxLanguage = "json" | "yaml" | "sql";

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
  const tokens = language === "json" ? jsonTokens(value) : language === "sql" ? sqlTokens(value) : yamlTokens(value);
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
    /#[^\n]*|^[ \t-]*[A-Za-z_][\w.-]*(?=\s*:)|"(?:\\.|[^"\\])*"|'(?:''|[^'])*'|\b(?:true|false|null|yes|no)\b|(?<![\w.-])-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?![\w.-])/gim,
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

// Lexical coloring only: DataFusion remains authoritative for SQL validation,
// function resolution and types. Unknown/UDF function calls are colored too.
const SQL_KEYWORDS = new Set(`ALL AND ANY ARRAY AS ASC BETWEEN BIGINT BOOLEAN BY CASE CAST CHAR CROSS
CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP DATE DECIMAL DESC DISTINCT DOUBLE ELSE END ESCAPE EXCEPT
EXISTS EXPLAIN EXTRACT FALSE FETCH FILTER FIRST FLOAT FOLLOWING FOR FROM FULL GROUP GROUPING HAVING
ILIKE IN INNER INT INTEGER INTERSECT INTERVAL INTO IS JOIN LAST LATERAL LEADING LEFT LIKE LIMIT
NOT NULL NULLS OFFSET ON OR ORDER OUTER OVER PARTITION PRECEDING RANGE REAL RECURSIVE RIGHT ROW ROWS
SELECT SMALLINT SOME STRING STRUCT THEN TIME TIMESTAMP TRAILING TRUE TRY_CAST UNBOUNDED UNION
UNNEST USING VALUES VARCHAR WHEN WHERE WINDOW WITH`.split(/\s+/));

function sqlTokens(value: string): Token[] {
  return tokenize(value,
    /--[^\r\n]*|\/\*[\s\S]*?(?:\*\/|$)|\$(\w*)\$[\s\S]*?\$\1\$|'(?:''|[^'])*(?:'|$)|"(?:""|[^"])*(?:"|$)|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|[\p{L}_][\p{L}\p{N}_$]*/gu,
    (text, offset) => {
      if (text.startsWith("--") || text.startsWith("/*")) return "comment";
      if (text.startsWith("'") || text.startsWith("$")) return "string";
      if (text.startsWith('"')) return "identifier";
      if (/^[\d.]/.test(text)) return "number";
      if (/^(true|false)$/i.test(text)) return "boolean";
      if (/^null$/i.test(text)) return "null";
      if (SQL_KEYWORDS.has(text.toUpperCase())) return "keyword";
      if (/^\s*\(/.test(value.slice(offset + text.length))) return "function";
      return "identifier";
    });
}

function tokenize(
  value: string,
  pattern: RegExp,
  classify: (text: string, offset: number) => string,
): Token[] {
  const tokens: Token[] = [];
  let offset = 0;
  for (const match of value.matchAll(pattern)) {
    const index = match.index ?? 0;
    if (index > offset) tokens.push({ text: value.slice(offset, index) });
    tokens.push({ text: match[0], kind: classify(match[0], index) });
    offset = index + match[0].length;
  }
  if (offset < value.length) tokens.push({ text: value.slice(offset) });
  return tokens;
}
