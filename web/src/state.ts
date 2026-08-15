import type {
  DeliveryRecord,
  JsonObject,
  RuntimeState,
  ValidationState,
} from "./types";

export interface EditorState {
  id?: string;
  persistedRevision?: number;
  editRevision: number;
  savedEditRevision?: number;
  name: string;
  description: string;
  config: JsonObject;
  validation: ValidationState;
  runtime: RuntimeState;
}

export type EditorAction =
  | { type: "new"; config: JsonObject }
  | { type: "open"; delivery: DeliveryRecord }
  | { type: "name"; name: string }
  | { type: "description"; description: string }
  | { type: "config"; config: JsonObject }
  | { type: "persisted"; delivery: DeliveryRecord }
  | { type: "runtime"; delivery: DeliveryRecord };

export function editorReducer(
  state: EditorState,
  action: EditorAction,
): EditorState {
  switch (action.type) {
    case "new":
      return {
        editRevision: 0,
        name: "",
        description: "",
        config: action.config,
        validation: { state: "draft" },
        runtime: { state: "stopped" },
      };
    case "open":
      return {
        id: action.delivery.id,
        persistedRevision: action.delivery.revision,
        editRevision: 0,
        savedEditRevision: 0,
        name: action.delivery.name,
        description: action.delivery.description,
        config: action.delivery.config,
        validation: action.delivery.validation,
        runtime: action.delivery.runtime,
      };
    case "name":
      return changed(state, { name: action.name });
    case "description":
      return changed(state, { description: action.description });
    case "config":
      return changed(state, { config: action.config });
    case "persisted":
      return {
        ...state,
        id: action.delivery.id,
        persistedRevision: action.delivery.revision,
        savedEditRevision: state.editRevision,
        validation: action.delivery.validation,
        runtime: action.delivery.runtime,
      };
    case "runtime":
      return {
        ...state,
        persistedRevision: action.delivery.revision,
        validation: action.delivery.validation,
        runtime: action.delivery.runtime,
      };
  }
}

function changed(
  state: EditorState,
  update: Partial<EditorState>,
): EditorState {
  return {
    ...state,
    ...update,
    editRevision: state.editRevision + 1,
    validation: { state: "draft" },
  };
}

export const isDirty = (state: EditorState): boolean =>
  state.savedEditRevision !== state.editRevision;

export const isReadOnly = (state: EditorState): boolean =>
  state.runtime.state === "running" ||
  state.runtime.state === "starting" ||
  state.runtime.state === "stopping";
