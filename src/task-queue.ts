type Task = () => Promise<any>;

export class TaskQueue {
  protected tasks: Task[];
  protected top: number;
  protected running: boolean;

  constructor() {
    this.tasks = [];
    this.top = 0;
    this.running = false;
  }

  public empty() {
    return this.top >= this.tasks.length;
  }

  public push(task: Task) {
    this.tasks.push(task);
    this.run();
  }

  protected pop() {
    const task = this.tasks[this.top++];
    if (this.top >= this.tasks.length) {
      this.tasks = [];
      this.top = 0;
    }
    return task;
  }

  protected async run() {
    if (this.running) {
      return;
    }
    this.running = true;
    while (!this.empty()) {
      const task = this.pop();
      try {
        await task();
      } catch {}
    }
    this.running = false;
  }

  public flush() {
    this.tasks = [];
    this.top = 0;
    return this;
  }
}
