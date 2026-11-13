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

import { calculateTotalTranslate3d } from "./panel-dock";

describe("calculateTotalTranslate3d", () => {
  it("should parse a single translate3d fragment", () => {
    expect(calculateTotalTranslate3d("translate3d(10px, -5px, 0px)")).toEqual([10, -5, 0]);
  });

  it("should parse two translate3d fragments by summing", () => {
    expect(calculateTotalTranslate3d("translate3d(10px, 20px, 0px) translate3d(5px, -8px, 3px)")).toEqual([15, 12, 3]);
  });

  it("should parse decimal values in translate3d fragment", () => {
    expect(calculateTotalTranslate3d("translate3d(1.5px, 2.25px, 0px)")).toEqual([1.5, 2.25, 0]);
  });

  it("should parse negative values in translate3d fragment", () => {
    expect(calculateTotalTranslate3d("translate3d(-10px, -20px, -3px)")).toEqual([-10, -20, -3]);
  });

  it("should return [0, 0, 0] when no translate3d pattern exists", () => {
    expect(calculateTotalTranslate3d("")).toEqual([0, 0, 0]);
    expect(calculateTotalTranslate3d("rotate(45deg)")).toEqual([0, 0, 0]);
  });

  it("should treat a translate3d fragment without px units as [0, 0, 0]", () => {
    expect(calculateTotalTranslate3d("translate3d(10, 20, 0)")).toEqual([0, 0, 0]);
  });
});
