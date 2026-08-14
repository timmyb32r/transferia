export interface LatestJobResult<T> {
  revision: number;
  value: T;
}

export class LatestJob<TInput, TOutput> {
  #controller: AbortController | undefined;
  #sequence = 0;

  cancel(): void {
    this.#sequence += 1;
    this.#controller?.abort();
    this.#controller = undefined;
  }

  async run(
    revision: number,
    input: TInput,
    operation: (input: TInput, signal: AbortSignal) => Promise<TOutput>,
  ): Promise<LatestJobResult<TOutput> | undefined> {
    this.cancel();
    const sequence = this.#sequence;
    const controller = new AbortController();
    this.#controller = controller;
    try {
      const value = await operation(input, controller.signal);
      if (controller.signal.aborted || sequence !== this.#sequence)
        return undefined;
      return { revision, value };
    } finally {
      if (this.#controller === controller) this.#controller = undefined;
    }
  }
}
