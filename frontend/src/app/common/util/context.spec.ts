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

import { ContextManager, ObservableContextManager } from "./context";

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
});

describe("ObservableContextManager", () => {
  it("should emit [exiting, entering] on the enter stream when a context is entered", () => {
    const manager = ObservableContextManager<string>("default");
    const enterEvents: [string, string][] = [];
    manager.getEnterStream().subscribe(event => enterEvents.push(event));

    manager.withContext("inner", () => {});

    expect(enterEvents).toEqual([["default", "inner"]]);
  });

  it("should emit [exiting, entering] on the exit stream when a context is exited", () => {
    const manager = ObservableContextManager<string>("default");
    const exitEvents: [string, string][] = [];
    manager.getExitStream().subscribe(event => exitEvents.push(event));

    manager.withContext("inner", () => {});

    expect(exitEvents).toEqual([["inner", "default"]]);
  });

  it("should emit the enter event after the context stack is updated", () => {
    const manager = ObservableContextManager<string>("default");
    const contextsAtEmission: string[] = [];
    manager.getEnterStream().subscribe(() => contextsAtEmission.push(manager.getContext()));

    manager.withContext("inner", () => {});

    // the new context is already on the stack when the enter event fires
    expect(contextsAtEmission).toEqual(["inner"]);
  });

  it("should emit the exit event after the context stack is popped", () => {
    const manager = ObservableContextManager<string>("default");
    const contextsAtEmission: string[] = [];
    manager.getExitStream().subscribe(() => contextsAtEmission.push(manager.getContext()));

    manager.withContext("inner", () => {});

    // the exited context is already off the stack when the exit event fires
    expect(contextsAtEmission).toEqual(["default"]);
  });

  it("should deliver both enter and exit events on the change context stream", () => {
    const manager = ObservableContextManager<string>("default");
    const changeEvents: [string, string][] = [];
    manager.getChangeContextStream().subscribe(event => changeEvents.push(event));

    manager.withContext("inner", () => {});

    expect(changeEvents).toEqual([
      ["default", "inner"],
      ["inner", "default"],
    ]);
  });

  it("should emit events in the correct order across nested withContext calls", () => {
    const manager = ObservableContextManager<string>("default");
    const events: { type: string; event: [string, string] }[] = [];
    manager.getEnterStream().subscribe(event => events.push({ type: "enter", event }));
    manager.getExitStream().subscribe(event => events.push({ type: "exit", event }));

    manager.withContext("outer", () => {
      manager.withContext("inner", () => {});
    });

    expect(events).toEqual([
      { type: "enter", event: ["default", "outer"] },
      { type: "enter", event: ["outer", "inner"] },
      { type: "exit", event: ["inner", "outer"] },
      { type: "exit", event: ["outer", "default"] },
    ]);
  });

  it("should emit enter and exit events when the callable throws", () => {
    const manager = ObservableContextManager<string>("default");
    const changeEvents: [string, string][] = [];
    manager.getChangeContextStream().subscribe(event => changeEvents.push(event));

    expect(() =>
      manager.withContext("inner", () => {
        throw new Error("callable failure");
      })
    ).toThrowError("callable failure");

    // the exit event still fires because withContext exits in a finally block
    expect(changeEvents).toEqual([
      ["default", "inner"],
      ["inner", "default"],
    ]);

    expect(manager.getContext()).toBe("default");
  });

  it("should still provide the basic ContextManager behavior", () => {
    const manager = ObservableContextManager<string>("default");

    const result = manager.withContext("inner", () => {
      expect(manager.getContext()).toBe("inner");
      expect(manager.prevContext()).toBe("default");
      return "value";
    });

    expect(result).toBe("value");
    expect(manager.getContext()).toBe("default");
  });

  it("should preserve object references through the stack and in emitted tuples", () => {
    // mirrors the real JointGraphContextType usage where the context is an object
    interface ObjectContext {
      readonly name: string;
    }
    const defaultContext: ObjectContext = { name: "default" };
    const innerContext: ObjectContext = { name: "inner" };
    const manager = ObservableContextManager<ObjectContext>(defaultContext);

    const enterEvents: [ObjectContext, ObjectContext][] = [];
    const exitEvents: [ObjectContext, ObjectContext][] = [];
    manager.getEnterStream().subscribe(event => enterEvents.push(event));
    manager.getExitStream().subscribe(event => exitEvents.push(event));

    // the same reference is returned for the default context
    expect(manager.getContext()).toBe(defaultContext);

    manager.withContext(innerContext, () => {
      // the exact inner reference is returned, not a copy
      expect(manager.getContext()).toBe(innerContext);
      expect(manager.prevContext()).toBe(defaultContext);
    });

    // the default reference is restored after exit
    expect(manager.getContext()).toBe(defaultContext);

    // emitted tuples carry the original object references
    expect(enterEvents[0][0]).toBe(defaultContext);
    expect(enterEvents[0][1]).toBe(innerContext);
    expect(exitEvents[0][0]).toBe(innerContext);
    expect(exitEvents[0][1]).toBe(defaultContext);
  });

  it("should unwind both levels via finally blocks when a nested callable throws", () => {
    const manager = ObservableContextManager<string>("default");
    const changeEvents: [string, string][] = [];
    manager.getChangeContextStream().subscribe(event => changeEvents.push(event));

    expect(() =>
      manager.withContext("outer", () =>
        manager.withContext("inner", () => {
          throw new Error("inner boom");
        })
      )
    ).toThrowError("inner boom");

    // the error propagated all the way out and the stack is fully restored
    expect(manager.getContext()).toBe("default");

    // both levels' exit events fire during unwinding via their finally blocks
    expect(changeEvents).toEqual([
      ["default", "outer"],
      ["outer", "inner"],
      ["inner", "outer"],
      ["outer", "default"],
    ]);
  });

  it("should not replay earlier events to a late subscriber", () => {
    const manager = ObservableContextManager<string>("default");

    // trigger a full enter/exit cycle before anyone subscribes
    manager.withContext("inner", () => {});

    const enterEvents: [string, string][] = [];
    manager.getEnterStream().subscribe(event => enterEvents.push(event));

    // the streams are plain Subjects (no replay), so the late subscriber sees nothing
    expect(enterEvents).toEqual([]);
  });

  it("should allow prevContext() to be called inside the enter subscriber", () => {
    const manager = ObservableContextManager<string>("default");
    const prevContexts: string[] = [];

    // at enter-emission time the new context is already pushed, so prevContext is valid
    manager.getEnterStream().subscribe(() => prevContexts.push(manager.prevContext()));

    manager.withContext("inner", () => {});

    expect(prevContexts).toEqual(["default"]);
  });

  it("should emit correctly for sequential sibling withContext calls", () => {
    const manager = ObservableContextManager<string>("default");
    const changeEvents: [string, string][] = [];
    manager.getChangeContextStream().subscribe(event => changeEvents.push(event));

    manager.withContext("A", () => {});
    manager.withContext("B", () => {});

    expect(changeEvents).toEqual([
      ["default", "A"],
      ["A", "default"],
      ["default", "B"],
      ["B", "default"],
    ]);
  });

  it("should handle entering the same context value as the current one", () => {
    const manager = ObservableContextManager<string>("default");
    const enterEvents: [string, string][] = [];
    manager.getEnterStream().subscribe(event => enterEvents.push(event));

    manager.withContext("default", () => {
      // dedup is positional (stack depth), not value-based
      expect(manager.getContext()).toBe("default");
      expect(manager.prevContext()).toBe("default");
    });

    expect(enterEvents).toEqual([["default", "default"]]);
  });

  it("should return falsy values produced by the callable unchanged", () => {
    const manager = ObservableContextManager<string>("default");

    // guards against an `|| fallback` regression in the return path
    expect(manager.withContext("inner", () => undefined)).toBeUndefined();
    expect(manager.withContext("inner", () => 0)).toBe(0);
    expect(manager.withContext("inner", () => null)).toBeNull();
    expect(manager.withContext("inner", () => false)).toBe(false);
  });
});
