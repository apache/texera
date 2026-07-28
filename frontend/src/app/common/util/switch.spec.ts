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

import { exhaustiveGuard } from "./switch";

describe("exhaustiveGuard", () => {
  it("always throws when reached at runtime", () => {
    // Cast is required because the signature only accepts `never`; a real
    // caller reaches this line when an unexpected value slips past a switch.
    expect(() => exhaustiveGuard("unexpected" as never)).toThrow();
  });

  it("includes the offending value in the error message", () => {
    expect(() => exhaustiveGuard("oops" as never)).toThrow(
      'ERROR! Reached forbidden guard function with unexpected value: "oops"'
    );
  });

  it("serializes object values with JSON.stringify in the message", () => {
    expect(() => exhaustiveGuard({ kind: "bad" } as never)).toThrow(
      'ERROR! Reached forbidden guard function with unexpected value: {"kind":"bad"}'
    );
  });

  it("acts as the default branch of an exhaustive switch", () => {
    type Shape = "circle" | "square";
    const area = (shape: Shape): number => {
      switch (shape) {
        case "circle":
          return 1;
        case "square":
          return 2;
        default:
          return exhaustiveGuard(shape);
      }
    };
    expect(area("circle")).toBe(1);
    expect(area("square")).toBe(2);
    // A value outside the union must trigger the guard's throw.
    expect(() => area("triangle" as Shape)).toThrow(/unexpected value/);
  });
});
