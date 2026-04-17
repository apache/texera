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

import * as Y from "yjs";
import * as _ from "lodash";
import { isDefined } from "../../common/util/predicate";

export type YTextify<T> = T extends string ? Y.Text : T;
export type YArrayify<T> = T extends Array<any> ? Y.Array<any> : T;

/**
 * <code>YType<T></code> is the yjs-object version of a normal js object with type <code>T</code>.
 *
 * Additionally, <code>YType</code> preserves <code>keyof</code> requirements from the original object.
 *
 * <code>toJSON()</code> converts the <code>YType</code> back to a normal js object.
 */
export type YType<T> = Omit<Y.AbstractType<any>, "get" | "set" | "has" | "toJSON"> & {
  get<TKey extends keyof T>(key: TKey): YArrayify<YTextify<T[TKey]>>;
  set<TKey extends keyof T>(key: TKey, value: YArrayify<YTextify<T[TKey]>>): void;
  has<TKey extends keyof T>(key: TKey): boolean;
  toJSON(): T;
};

/** Creates a <code>YType</code> given a normal object. Returns either a <code>YType</code>,
 *  or the original object if it is a primitive type other than string, because string will be converted to
 *  <code>Y.Text</code>.
 *  @param obj: a normal object, could be either a string, an array, or a complicated object with its own attributes.
 *  Note it is NOT supposed to be a primitive type (if you pass a primitive type into this function the TS code will not
 *  compile), but we handle the case of primitive type and return it as-is because we do the conversion recursively
 *  to the deepest level in the obj using this same function, so during runtime this function <b>might</b> be called
 *  on primitive types.
 */
export function createYTypeFromObject<T extends object>(obj: T): YType<T> {
  if (obj === null || obj === undefined) return obj;
  const originalType = typeof (obj as any);
  switch (originalType) {
    case "bigint":
    case "boolean":
    case "function":
    case "number":
    case "symbol":
    case "undefined":
      return obj as any;
    case "string":
      return new Y.Text(obj as unknown as string) as unknown as YType<T>;
    case "object": {
      const objType = obj.constructor.name;
      if (objType === "String") {
        return new Y.Text(obj as unknown as string) as unknown as YType<T>;
      } else if (objType === "Array") {
        const yArray = new Y.Array();
        // Create YType for each array item and push
        for (const item of obj as any) {
          if (isDefined(item)) yArray.push([createYTypeFromObject(item) as unknown]);
        }
        return yArray as unknown as YType<T>;
      } else if (objType === "Object") {
        // return new
        const yMap = new Y.Map();
        Object.keys(obj).forEach((k: string) => {
          const value = obj[k as keyof T] as any as object;
          if (value !== undefined) {
            yMap.set(k, createYTypeFromObject(value));
          }
        });
        return yMap as unknown as YType<T>;
      } else {
        // All other objects that cannot be processed.
        throw TypeError(`Cannot create YType from ${objType}!`);
      }
    }
  }
}

/**
 * Updates a <code>YType</code> in-place given a new <b>normal object</b> version of this <code>YType</code>.
 * @param oldYObj The old <code>YType</code> to be updated.
 * @param newObj The new normal object, must be the same template type as the <code>YType</code> to be updated.
 */
export function updateYTypeFromObject<T extends object>(oldYObj: YType<T>, newObj: T): boolean {
  if (newObj === null || newObj === undefined || oldYObj === null || oldYObj === undefined) return false;
  const originalNewObjType = typeof newObj;
  switch (originalNewObjType) {
    case "bigint":
    case "boolean":
    case "number":
    case "symbol":
    case "undefined":
    case "function":
      return false;
    case "string": {
      const yText = oldYObj as unknown as Y.Text;
      if (yText.toJSON() !== (newObj as unknown as string)) {
        // Inplace update.
        yText.delete(0, yText.length);
        yText.insert(0, newObj as unknown as string);
      }
      return true;
    }
    case "object":
      break;
  }
  const newObjType = newObj.constructor.name;
  const oldObjType = oldYObj.toJSON().constructor.name;
  if (newObjType !== oldObjType) return false;
  if (newObjType === "String") {
    const yText = oldYObj as unknown as Y.Text;
    if (yText.toJSON() !== (newObj as unknown as string)) {
      // Inplace update.
      yText.delete(0, yText.length);
      yText.insert(0, newObj as unknown as string);
    }
  } else if (newObjType === "Array") {
    const oldYObjAsYArray = oldYObj as unknown as Y.Array<any>;
    const newObjAsArr = newObj as any[];
    const oldObjAsArr = oldYObjAsYArray.toJSON();
    const oldArrLen = oldObjAsArr.length;
    const newArrLen = newObjAsArr.length;

    const toYValue = (value: any) => {
      const res = createYTypeFromObject(value);
      return res === undefined ? null : res;
    };

    // lcsLengthTable[i][j] = longest common subsequence length between
    // oldObjAsArr[i:] and newObjAsArr[j:].
    const lcsLengthTable: number[][] = Array.from({ length: oldArrLen + 1 }, () => Array(newArrLen + 1).fill(0));

    for (let oldIndex = oldArrLen - 1; oldIndex >= 0; oldIndex--) {
      for (let newIndex = newArrLen - 1; newIndex >= 0; newIndex--) {
        if (_.isEqual(oldObjAsArr[oldIndex], newObjAsArr[newIndex])) {
          lcsLengthTable[oldIndex][newIndex] = lcsLengthTable[oldIndex + 1][newIndex + 1] + 1;
        } else {
          lcsLengthTable[oldIndex][newIndex] = Math.max(
            lcsLengthTable[oldIndex + 1][newIndex],
            lcsLengthTable[oldIndex][newIndex + 1]
          );
        }
      }
    }

    // Recover aligned equal positions.
    const matchedIndexPairs: Array<[number, number]> = [];
    let oldIndex = 0;
    let newIndex = 0;

    while (oldIndex < oldArrLen && newIndex < newArrLen) {
      if (_.isEqual(oldObjAsArr[oldIndex], newObjAsArr[newIndex])) {
        matchedIndexPairs.push([oldIndex, newIndex]);
        oldIndex++;
        newIndex++;
      } else if (lcsLengthTable[oldIndex + 1][newIndex] >= lcsLengthTable[oldIndex][newIndex + 1]) {
        oldIndex++;
      } else {
        newIndex++;
      }
    }

    // Build unmatched segments between aligned equal positions.
    const unmatchedSegments: Array<{
      oldStartIndex: number;
      oldEndIndex: number;
      newStartIndex: number;
      newEndIndex: number;
    }> = [];

    let nextOldSegmentStart = 0;
    let nextNewSegmentStart = 0;

    for (const [matchedOldIndex, matchedNewIndex] of matchedIndexPairs) {
      if (nextOldSegmentStart < matchedOldIndex || nextNewSegmentStart < matchedNewIndex) {
        unmatchedSegments.push({
          oldStartIndex: nextOldSegmentStart,
          oldEndIndex: matchedOldIndex,
          newStartIndex: nextNewSegmentStart,
          newEndIndex: matchedNewIndex,
        });
      }

      nextOldSegmentStart = matchedOldIndex + 1;
      nextNewSegmentStart = matchedNewIndex + 1;
    }

    if (nextOldSegmentStart < oldArrLen || nextNewSegmentStart < newArrLen) {
      unmatchedSegments.push({
        oldStartIndex: nextOldSegmentStart,
        oldEndIndex: oldArrLen,
        newStartIndex: nextNewSegmentStart,
        newEndIndex: newArrLen,
      });
    }

    // Apply from right to left so array indices remain stable.
    for (let segmentIndex = unmatchedSegments.length - 1; segmentIndex >= 0; segmentIndex--) {
      const { oldStartIndex, oldEndIndex, newStartIndex, newEndIndex } = unmatchedSegments[segmentIndex];

      const oldSegmentLength = oldEndIndex - oldStartIndex;
      const newSegmentLength = newEndIndex - newStartIndex;
      const overlappingLength = Math.min(oldSegmentLength, newSegmentLength);

      // Update overlapping items in place where possible.
      for (let segmentOffset = overlappingLength - 1; segmentOffset >= 0; segmentOffset--) {
        const arrayIndex = oldStartIndex + segmentOffset;
        const newValue = newObjAsArr[newStartIndex + segmentOffset];

        if (!_.isEqual(oldObjAsArr[arrayIndex], newValue)) {
          if (!updateYTypeFromObject(oldYObjAsYArray.get(arrayIndex), newValue)) {
            oldYObjAsYArray.delete(arrayIndex, 1);
            oldYObjAsYArray.insert(arrayIndex, [toYValue(newValue)]);
          }
        }
      }

      // Delete remaining old items in this segment.
      if (oldSegmentLength > newSegmentLength) {
        oldYObjAsYArray.delete(oldStartIndex + overlappingLength, oldSegmentLength - newSegmentLength);
      }

      // Insert remaining new items in this segment.
      if (newSegmentLength > oldSegmentLength) {
        const insertedYValues = newObjAsArr.slice(newStartIndex + overlappingLength, newEndIndex).map(toYValue);

        oldYObjAsYArray.insert(oldStartIndex + overlappingLength, insertedYValues);
      }
    }
  } else if (newObjType === "Object") {
    const oldYObjAsYMap = oldYObj as unknown as Y.Map<any>;
    const oldObj = oldYObjAsYMap.toJSON() as T;
    const keySet = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);
    keySet.forEach((k: string) => {
      const newValue = newObj[k as keyof T] as any;
      if (!_.isEqual(oldObj[k as keyof T], newValue)) {
        if (!updateYTypeFromObject(oldYObjAsYMap.get(k), newValue)) {
          if (newValue !== undefined) {
            oldYObjAsYMap.set(k, createYTypeFromObject(newValue));
          }
        }
      }
    });
  } else {
    return false;
  }
  return true;
}
