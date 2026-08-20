import { searchMatchIndices } from "./search";

export function SearchHighlight({ text, query }: { text: string; query: string }) {
  const matched = new Set(searchMatchIndices(text, query));
  return (
    <>
      {[...text].map((character, index) =>
        matched.has(index) ? <strong key={index}>{character}</strong> : character,
      )}
    </>
  );
}
