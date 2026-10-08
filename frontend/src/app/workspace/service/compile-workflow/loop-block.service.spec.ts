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

import { TestBed } from "@angular/core/testing";
import { LoopBlockService } from "./loop-block.service";

describe("LoopBlockService", () => {
  let service: LoopBlockService;

  beforeEach(() => {
    service = TestBed.inject(LoopBlockService);
  });

  it("counts no operator inside a loop block before the first compile result", () => {
    expect(service.getEnclosingLoopStarts("body")).toEqual([]);
  });

  it("reports the Loop Starts a compile result puts around an operator, and none around one it leaves out", () => {
    service.setOperatorLoopStarts({ body: ["start"] });
    expect(service.getEnclosingLoopStarts("body")).toEqual(["start"]);
    expect(service.getEnclosingLoopStarts("start")).toEqual([]);
    expect(service.getEnclosingLoopStarts("outside")).toEqual([]);
  });

  it("orders nested blocks' Loop Starts outermost first, whatever order the compile result gives them in", () => {
    // outer -> inner -> body -> ...: the inner Loop Start is itself inside the outer block
    service.setOperatorLoopStarts({ body: ["a-inner", "b-outer"], "a-inner": ["b-outer"] });
    expect(service.getEnclosingLoopStarts("body")).toEqual(["b-outer", "a-inner"]);
    // blocks that do not nest keep the compile result's order
    service.setOperatorLoopStarts({ body: ["s2", "s1"] });
    expect(service.getEnclosingLoopStarts("body")).toEqual(["s2", "s1"]);
  });

  it("replaces the previous compile result with each new one", () => {
    service.setOperatorLoopStarts({ body: ["start"] });
    service.setOperatorLoopStarts({});
    expect(service.getEnclosingLoopStarts("body")).toEqual([]);
  });

  it("does not take an operator id for a property of a plain object", () => {
    service.setOperatorLoopStarts({});
    expect(service.getEnclosingLoopStarts("constructor")).toEqual([]);
    expect(service.getEnclosingLoopStarts("__proto__")).toEqual([]);
  });

  it("announces every compile result, including one that leaves every block as it was", () => {
    let announcements = 0;
    const subscription = service.getCompileResultStream().subscribe(() => announcements++);
    service.setOperatorLoopStarts({ body: ["start"] });
    service.setOperatorLoopStarts({ body: ["start"] });
    expect(announcements).toBe(2);
    subscription.unsubscribe();
  });
});
