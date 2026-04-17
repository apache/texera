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

function parseTranslate3d(translate3d: string): [number, number, number] {
  const regex = /translate3d\((-?\d+\.?\d*)px,\s*(-?\d+\.?\d*)px,\s*(-?\d+\.?\d*)px\)/g;
  const match = regex.exec(translate3d);
  if (match) {
    const x = parseFloat(match[1]);
    const y = parseFloat(match[2]);
    const z = parseFloat(match[3]);
    return [x, y, z];
  }
  return [0, 0, 0];
}

export function calculateTotalTranslate3d(translates: string): [number, number, number] {
  let totalXOffset = 0;
  let totalYOffset = 0;
  let totalZOffset = 0;

  const translate3dArray = translates.match(/translate3d\(.*?\)/g) || [];

  for (const translate of translate3dArray) {
    const [x, y, z] = parseTranslate3d(translate);
    totalXOffset += x;
    totalYOffset += y;
    totalZOffset += z;
  }

  return [totalXOffset, totalYOffset, totalZOffset];
}
