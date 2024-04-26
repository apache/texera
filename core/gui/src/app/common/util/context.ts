import { merge, Subject } from "rxjs";

export function ContextManager<Context>(defaultContext: Context) {
  abstract class ContextManager {
    private static contextStack: Context[] = [defaultContext];

    public static getContext() {
      return this.contextStack[this.contextStack.length - 1];
    }

    public static prevContext() {
      if (this.contextStack.length < 2) {
        throw new Error("No previous context to get (you are in the default context already)");
      }
      return this.contextStack[this.contextStack.length - 2];
    }

    public static withContext<T>(context: Context, callable: () => T): T {
      try {
        this.enter(context);
        return callable();
      } finally {
        this.exit();
      }
    }

    protected static enter(context: Context) {
      this.contextStack.push(context);
    }

    protected static exit() {
      this.contextStack.pop();
    }
  }

  return ContextManager;
}

export function ObservableContextManager<Context>(defaultContext: Context) {
  abstract class ObservableContextManager extends ContextManager(defaultContext) {
    private static enterStream = new Subject<[exiting: Context, entering: Context]>();
    private static exitStream = new Subject<[exiting: Context, entering: Context]>();
    private static changeContextStream = ObservableContextManager.createChangeContextStream();

    public static getEnterStream() {
      return this.enterStream.asObservable();
    }

    public static getExitStream() {
      return this.exitStream.asObservable();
    }

    public static getChangeContextStream() {
      return this.changeContextStream;
    }

    private static createChangeContextStream() {
      return merge(this.getEnterStream(), this.getExitStream());
    }

    protected static enter(context: Context): void {
      const oldContext = this.getContext();
      const newContext = context;
      super.enter(context);
      this.enterStream.next([oldContext, newContext]);
    }

    protected static exit(): void {
      const oldContext = this.getContext();
      super.exit();
      const newContext = this.getContext();
      this.exitStream.next([oldContext, newContext]);
    }
  }
  return ObservableContextManager;
}
