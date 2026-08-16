export function createColumnDragPreview(
  row: HTMLTableRowElement,
  dataTransfer: DataTransfer,
  clientX: number,
  clientY: number,
): HTMLTableElement {
  const bounds = row.getBoundingClientRect();
  const table = document.createElement("table");
  const body = document.createElement("tbody");
  const clone = row.cloneNode(true) as HTMLTableRowElement;
  const sourceInputs = row.querySelectorAll<HTMLInputElement>("input");
  const clonedInputs = clone.querySelectorAll<HTMLInputElement>("input");

  sourceInputs.forEach((input, index) => {
    const cloned = clonedInputs[index];
    if (cloned === undefined) return;
    cloned.value = input.value;
    cloned.checked = input.checked;
  });
  clone.classList.remove("dragged", "drag-before", "drag-after");
  table.className = "config-table column-table column-drag-preview";
  table.style.width = `${bounds.width}px`;
  body.append(clone);
  table.append(body);
  document.body.append(table);
  dataTransfer.setDragImage(
    table,
    Math.max(0, clientX - bounds.left),
    Math.max(0, clientY - bounds.top),
  );
  return table;
}

export function insertionSlot(event: DragEvent, index: number): number {
  const bounds = (
    event.currentTarget as HTMLTableRowElement
  ).getBoundingClientRect();
  if (bounds.height === 0) return index + 1;
  return event.clientY > bounds.top + bounds.height / 2 ? index + 1 : index;
}
