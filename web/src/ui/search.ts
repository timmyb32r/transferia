const RUSSIAN_LAYOUT = "йцукенгшщзхъфывапролджэячсмитьбю";
const LATIN_LAYOUT = "qwertyuiop[]asdfghjkl;'zxcvbnm,.";

export function latinKeyboardInput(value: string): string {
  return [...value.toLowerCase()]
    .map((character) => {
      const index = RUSSIAN_LAYOUT.indexOf(character);
      return index === -1 ? character : (LATIN_LAYOUT[index] ?? character);
    })
    .join("");
}

function isSubsequence(value: string, query: string): boolean {
  let queryIndex = 0;
  for (const character of value) {
    if (character === query[queryIndex]) queryIndex += 1;
    if (queryIndex === query.length) return true;
  }
  return query.length === 0;
}

function rankNormalized(value: string, query: string): number | undefined {
  if (value.startsWith(query)) return 0;
  if (value.includes(query)) return 1;
  if (isSubsequence(value, query)) return 2;
  return undefined;
}

function matchNormalized(value: string, query: string): number[] | undefined {
  if (query === "") return [];
  const valueCharacters = [...value];
  const queryCharacters = [...query];
  for (let start = 0; start <= valueCharacters.length - queryCharacters.length; start += 1) {
    if (
      queryCharacters.every(
        (character, offset) => valueCharacters[start + offset] === character,
      )
    ) {
      return queryCharacters.map((_, offset) => start + offset);
    }
  }
  const indices: number[] = [];
  let queryIndex = 0;
  for (const [index, character] of valueCharacters.entries()) {
    if (character !== queryCharacters[queryIndex]) continue;
    indices.push(index);
    queryIndex += 1;
    if (queryIndex === queryCharacters.length) return indices;
  }
  return undefined;
}

export function searchMatchIndices(value: string, query: string): number[] {
  const normalizedValue = value.toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery === "") return [];
  const candidates = [
    ...new Set([normalizedQuery, latinKeyboardInput(normalizedQuery)]),
  ];
  let best: { rank: number; indices: number[] } | undefined;
  for (const candidate of candidates) {
    const rank = rankNormalized(normalizedValue, candidate);
    const indices = matchNormalized(normalizedValue, candidate);
    if (rank !== undefined && indices !== undefined && (best === undefined || rank < best.rank)) {
      best = { rank, indices };
    }
  }
  return best?.indices ?? [];
}

export function searchRank(value: string, query: string): number | undefined {
  const normalizedValue = value.toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery === "") return 0;

  const candidates = new Set([
    normalizedQuery,
    latinKeyboardInput(normalizedQuery),
  ]);
  let best: number | undefined;
  for (const candidate of candidates) {
    const rank = rankNormalized(normalizedValue, candidate);
    if (rank !== undefined && (best === undefined || rank < best)) best = rank;
  }
  return best;
}

export function matchesSearch(value: string, query: string): boolean {
  return searchRank(value, query) !== undefined;
}

export function rankSearchResults<T>(
  values: readonly T[],
  query: string,
  label: (value: T) => string,
): T[] {
  if (query.trim() === "") return [...values];
  return values
    .map((value, index) => ({
      value,
      index,
      rank: searchRank(label(value), query),
    }))
    .filter(
      (candidate): candidate is { value: T; index: number; rank: number } =>
        candidate.rank !== undefined,
    )
    .sort(
      (left, right) =>
        left.rank - right.rank ||
        label(left.value).localeCompare(label(right.value)) ||
        left.index - right.index,
    )
    .map((candidate) => candidate.value);
}

export function backendSearchQuery(query: string): string {
  const converted = latinKeyboardInput(query);
  return converted === query.toLowerCase() ? query : converted;
}

export function pathBrowseQuery(query: string): string {
  const converted = backendSearchQuery(query);
  if (converted.endsWith("/")) return converted;
  const separator = converted.lastIndexOf("/");
  return separator === -1 ? "" : converted.slice(0, separator + 1);
}

export function pathSearchFragment(query: string): string {
  const converted = backendSearchQuery(query);
  if (converted.endsWith("/")) return "";
  const separator = converted.lastIndexOf("/");
  return converted.slice(separator + 1);
}
