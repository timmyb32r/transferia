import { LatestJob } from "../effects";

export type TaskScope = "global" | "session" | "revision";

export class TaskRegistry {
  readonly #tasks = new Map<TaskScope, Set<{ cancel(): void }>>();

  latest<TContext, TInput, TOutput>(
    scope: TaskScope,
  ): LatestJob<TContext, TInput, TOutput> {
    const task = new LatestJob<TContext, TInput, TOutput>();
    const scoped = this.#tasks.get(scope) ?? new Set();
    scoped.add(task);
    this.#tasks.set(scope, scoped);
    return task;
  }

  cancel(scope: TaskScope): void {
    for (const task of this.#tasks.get(scope) ?? []) task.cancel();
  }

  cancelAll(): void {
    for (const scope of this.#tasks.keys()) this.cancel(scope);
  }
}
