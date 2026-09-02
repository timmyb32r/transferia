import type {
  DeliveryRecord,
  JsonObject,
  RuntimeState,
  ValidationState,
} from "./types";

export interface EditorState {
  sessionId: EditorSessionId;
  editing: boolean;
  id?: string;
  persistedRevision?: number;
  recordVersion?: string;
  localRevision: number;
  savedLocalRevision?: number;
  name: string;
  description: string;
  config: JsonObject;
  validation: ValidationState;
  runtime: RuntimeState;
}

export type EditorSessionId = string;

export type EditorAction =
  | { type: "new"; sessionId: EditorSessionId; config: JsonObject }
  | {
      type: "clone";
      sessionId: EditorSessionId;
      name: string;
      description: string;
      config: JsonObject;
    }
  | {
      type: "open";
      sessionId: EditorSessionId;
      delivery: DeliveryRecord;
    }
  | { type: "name"; name: string }
  | { type: "description"; description: string }
  | { type: "config"; config: JsonObject }
  | { type: "edit" }
  | {
      type: "persisted";
      sessionId: EditorSessionId;
      savedLocalRevision: number;
      delivery: DeliveryRecord;
    }
  | {
      type: "runtime";
      sessionId: EditorSessionId;
      expectedLocalRevision: number;
      delivery: DeliveryRecord;
    };

export function editorReducer(
  state: EditorState,
  action: EditorAction,
): EditorState {
  switch (action.type) {
    case "new":
      return {
        sessionId: action.sessionId,
        editing: true,
        localRevision: 0,
        name: "",
        description: "",
        config: action.config,
        validation: { state: "draft" },
        runtime: { state: "stopped" },
      };
    case "clone":
      return {
        sessionId: action.sessionId,
        editing: true,
        localRevision: 0,
        name: action.name,
        description: action.description,
        config: action.config,
        validation: { state: "draft" },
        runtime: { state: "stopped" },
      };
    case "open":
      return {
        sessionId: action.sessionId,
        editing: false,
        id: action.delivery.id,
        persistedRevision: action.delivery.revision,
        recordVersion: action.delivery.record_version,
        localRevision: 0,
        savedLocalRevision: 0,
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
    case "edit":
      return { ...state, editing: true };
    case "persisted":
      if (
        action.sessionId !== state.sessionId ||
        (state.id !== undefined && action.delivery.id !== state.id) ||
        (state.recordVersion !== undefined &&
          olderToken(action.delivery.record_version, state.recordVersion)) ||
        (state.persistedRevision !== undefined &&
          action.delivery.revision < state.persistedRevision)
      )
        return state;
      return {
        ...state,
        id: action.delivery.id,
        persistedRevision: action.delivery.revision,
        recordVersion: action.delivery.record_version,
        savedLocalRevision: action.savedLocalRevision,
        config:
          action.savedLocalRevision === state.localRevision
            ? action.delivery.config
            : state.config,
        editing:
          action.savedLocalRevision === state.localRevision
            ? false
            : state.editing,
        validation:
          action.savedLocalRevision === state.localRevision
            ? action.delivery.validation
            : state.validation,
        runtime: action.delivery.runtime,
      };
    case "runtime":
      if (
        action.sessionId !== state.sessionId ||
        action.expectedLocalRevision !== state.localRevision ||
        action.delivery.id !== state.id ||
        (state.recordVersion !== undefined &&
          olderToken(action.delivery.record_version, state.recordVersion)) ||
        (state.persistedRevision !== undefined &&
          action.delivery.revision < state.persistedRevision)
      )
        return state;
      return {
        ...state,
        persistedRevision: action.delivery.revision,
        recordVersion: action.delivery.record_version,
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
    localRevision: state.localRevision + 1,
    validation: { state: "draft" },
  };
}

export const isDirty = (state: EditorState): boolean =>
  state.savedLocalRevision !== state.localRevision;

export const isReadOnly = (state: EditorState): boolean =>
  !state.editing ||
  state.runtime.state === "running" ||
  state.runtime.state === "starting" ||
  state.runtime.state === "stopping";

function olderToken(candidate: string, current: string): boolean {
  return BigInt(candidate) < BigInt(current);
}
