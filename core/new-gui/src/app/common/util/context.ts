
export function ContextManager<Context>(defaultContext: Context) {
    abstract class ContextManager {
        private static contextStack: Context[] = [defaultContext];
        public static getContext() {
            return this.contextStack[this.contextStack.length-1];
        }
        public static enter(context: Context) {
            this.contextStack.push(context);
        }
        public static exit() {
            this.contextStack.pop();
        }
        public static withContext<T>(context: Context, callable: () => T): T{
            try {
                this.enter(context);
                return callable();
            } finally {
                this.exit();
            }
        }
    }

    return ContextManager;
}