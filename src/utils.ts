export namespace ErrorUtils {
  export interface JSONError {
    name: string;
    message: string;
    code?: string | number;
    cause?: any;
    response?: any;
    stack?: string[];
  }

  export function stack(e: unknown) {
    if (e instanceof Error && e.stack) {
      return e.stack
        .trim()
        .split('\n')
        .map(line => line.trim());
    }
    return [];
  }

  export function json(e: unknown, st = false) {
    const obj = {
      name: 'UnknownError',
      message: 'Unknown error',
    } as JSONError;
    try {
      // @ts-ignore
      if (typeof e === 'object' && e !== null && e.isAxiosError) {
        // @ts-ignore
        obj.name = e.name;
        // @ts-ignore
        obj.message = e.message;
        // @ts-ignore
        obj.code = e.response?.status;
        // @ts-ignore
        obj.response = e.response?.data;
        if (st) {
          obj['stack'] = stack(e);
        }
        return obj;
      }
      if (e instanceof Error) {
        obj.name = e.name;
        obj.message = e.message;
        if (st) {
          obj['stack'] = stack(e);
        }
        return obj;
      }
      obj.message = JSON.stringify(e, undefined, 0);
      return obj;
    } catch {
      return obj;
    }
  }
}

export namespace TimerUtils {
  export function delay(ms: number) {
    return new Promise<void>(resolve => setTimeout(resolve, ms));
  }
}
