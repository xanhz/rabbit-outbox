export class Logger {
  constructor(public readonly name: string) {}

  private print(level: string, msg: any, ...args: any[]) {
    if (msg instanceof Error) {
      console.log(`[${new Date().toISOString()}] - [${level.toUpperCase()}] - [${this.name}]:`, msg, ...args);
    } else {
      console.log(`[${new Date().toISOString()}] - [${level.toUpperCase()}] - [${this.name}]: ${msg}`, ...args);
    }
  }

  public info(msg: string, ...args: any[]) {
    this.print('info', msg, ...args);
  }

  public error(err: any, ...args: any[]) {
    this.print('error', err, ...args);
  }

  public warn(msg: string, ...args: any[]) {
    this.print('warn', msg, ...args);
  }

  public debug(msg: string, ...args: any[]) {
    if (process.env.DEBUG === 'true') {
      this.print('debug', msg, ...args);
    }
  }
}
