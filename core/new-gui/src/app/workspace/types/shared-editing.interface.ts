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
    // TODO: Fix this
    const oldYObjAsYArray = oldYObj as unknown as Y.Array<any>;
    const newObjAsArr = newObj as any[];
    const newArrLen = newObjAsArr.length;
    const oldObjAsArr = oldYObjAsYArray.toJSON();
    const oldArrLen = oldObjAsArr.length;
    // TODO: in-place update, assuming only one update at a time can happen.
    if (newArrLen < oldArrLen) {
      let i = 0;
      for (i; i < newArrLen; i++) {
        if (!_.isEqual(oldObjAsArr[i], newObjAsArr[i])) break;
      }
      oldYObjAsYArray.delete(i);
    } else if (newArrLen > oldArrLen) {
      let i = 0;
      for (i; i < newArrLen; i++) {
        if (!_.isEqual(oldObjAsArr[i], newObjAsArr[i])) break;
      }
      oldYObjAsYArray.insert(i, [createYTypeFromObject(newObjAsArr[i])]);
    } else {
      for (let i = 0; i < newArrLen; i++) {
        if (!_.isEqual(oldObjAsArr[i], newObjAsArr[i])) {
          if (!updateYTypeFromObject(oldYObjAsYArray.get(i), newObjAsArr[i])) {
            if (newObjAsArr[i] !== undefined) {
              oldYObjAsYArray.delete(i, 1);
              const res = createYTypeFromObject(newObjAsArr[i]);
              if (res === undefined) oldYObjAsYArray.insert(i, [null]);
              else oldYObjAsYArray.insert(i, [res]);
            }
          }
        }
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
