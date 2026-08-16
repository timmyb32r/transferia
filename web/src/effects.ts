export interface LatestJobResult<TContext, T> {
  requestId: number;
  context: TContext;
  value: T;
}

export class LatestJob<TContext, TInput, TOutput> {
  #controller: AbortController | undefined;
  #sequence = 0;

  cancel(): void {
    this.#sequence += 1;
    this.#controller?.abort();
    this.#controller = undefined;
  }

  async run(
    context: TContext,
    input: TInput,
    operation: (input: TInput, signal: AbortSignal) => Promise<TOutput>,
  ): Promise<LatestJobResult<TContext, TOutput> | undefined> {
    this.cancel();
    const requestId = this.#sequence;
    const controller = new AbortController();
    this.#controller = controller;
    try {
      const value = await operation(input, controller.signal);
      if (controller.signal.aborted || requestId !== this.#sequence)
        return undefined;
      return { requestId, context, value };
    } catch (reason) {
      if (controller.signal.aborted || requestId !== this.#sequence)
        return undefined;
      throw reason;
    } finally {
      if (this.#controller === controller) this.#controller = undefined;
    }
  }
}
