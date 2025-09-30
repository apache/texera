/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
