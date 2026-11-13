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

import { ContextManager } from "./context";

describe("ContextManager", () => {
  it("should return the default context initially", () => {
    // each factory call creates a new class with its own static context stack
    const manager = ContextManager<string>("default");

    expect(manager.getContext()).toBe("default");
  });

  it("should throw when prevContext is called in the default context", () => {
    const manager = ContextManager<string>("default");

    expect(() => manager.prevContext()).toThrowError(
      "No previous context to get (you are in the default context already)"
    );
  });

  it("should expose the entered context and the previous context inside withContext", () => {
    const manager = ContextManager<string>("default");

    manager.withContext("inner", () => {
      expect(manager.getContext()).toBe("inner");
      expect(manager.prevContext()).toBe("default");
    });
  });

  it("should restore the default context after withContext completes", () => {
    const manager = ContextManager<string>("default");

    manager.withContext("inner", () => {});

    expect(manager.getContext()).toBe("default");
    expect(() => manager.prevContext()).toThrowError(
      "No previous context to get (you are in the default context already)"
    );
  });

  it("should return the value returned by the callable", () => {
    const manager = ContextManager<string>("default");

    const result = manager.withContext("inner", () => 42);

    expect(result).toBe(42);
  });

  it("should restore the context and re-throw when the callable throws", () => {
    const manager = ContextManager<string>("default");

    expect(() =>
      manager.withContext("inner", () => {
        throw new Error("callable failure");
      })
    ).toThrowError("callable failure");

    // the context stack must be restored even though the callable threw
    expect(manager.getContext()).toBe("default");
  });

  it("should restore each level correctly for nested withContext calls", () => {
    const manager = ContextManager<string>("default");

    manager.withContext("outer", () => {
      expect(manager.getContext()).toBe("outer");
      expect(manager.prevContext()).toBe("default");

      manager.withContext("inner", () => {
        expect(manager.getContext()).toBe("inner");
        expect(manager.prevContext()).toBe("outer");
      });

      // back to the outer context after the inner scope exits
      expect(manager.getContext()).toBe("outer");
      expect(manager.prevContext()).toBe("default");
    });

    expect(manager.getContext()).toBe("default");
  });

  it("should keep context stacks of separately created managers isolated", () => {
    const managerA = ContextManager<string>("defaultA");
    const managerB = ContextManager<string>("defaultB");

    managerA.withContext("innerA", () => {
      expect(managerA.getContext()).toBe("innerA");
      // managerB must not be affected by managerA entering a context
      expect(managerB.getContext()).toBe("defaultB");
    });
  });

  it("should preserve object references through the stack", () => {
    // mirrors the real JointGraphContextType usage where the context is an object
    interface ObjectContext {
      readonly name: string;
    }
    const defaultContext: ObjectContext = { name: "default" };
    const innerContext: ObjectContext = { name: "inner" };
    const manager = ContextManager<ObjectContext>(defaultContext);

    // the same reference is returned for the default context
    expect(manager.getContext()).toBe(defaultContext);

    manager.withContext(innerContext, () => {
      // the exact inner reference is returned, not a copy
      expect(manager.getContext()).toBe(innerContext);
      expect(manager.prevContext()).toBe(defaultContext);
    });

    // the default reference is restored after exit
    expect(manager.getContext()).toBe(defaultContext);
  });

  it("should unwind both levels via finally blocks when a nested callable throws", () => {
    const manager = ContextManager<string>("default");

    expect(() =>
      manager.withContext("outer", () =>
        manager.withContext("inner", () => {
          throw new Error("inner boom");
        })
      )
    ).toThrowError("inner boom");

    // the error propagated all the way out and the stack is fully restored
    expect(manager.getContext()).toBe("default");
  });

  it("should handle entering the same context value as the current one", () => {
    const manager = ContextManager<string>("default");

    manager.withContext("default", () => {
      // dedup is positional (stack depth), not value-based
      expect(manager.getContext()).toBe("default");
      expect(manager.prevContext()).toBe("default");
    });
  });

  it("should return falsy values produced by the callable unchanged", () => {
    const manager = ContextManager<string>("default");

    // guards against an `|| fallback` regression in the return path
    expect(manager.withContext("inner", () => undefined)).toBeUndefined();
    expect(manager.withContext("inner", () => 0)).toBe(0);
    expect(manager.withContext("inner", () => null)).toBeNull();
    expect(manager.withContext("inner", () => false)).toBe(false);
  });
});
