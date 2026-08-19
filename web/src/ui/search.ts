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

export function matchesSearch(value: string, query: string): boolean {
  const normalizedValue = value.toLowerCase();
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery === "") return true;
  return (
    normalizedValue.includes(normalizedQuery) ||
    normalizedValue.includes(latinKeyboardInput(normalizedQuery))
  );
}

export function backendSearchQuery(query: string): string {
  const converted = latinKeyboardInput(query);
  return converted === query.toLowerCase() ? query : converted;
}
