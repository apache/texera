import * as Y from "yjs";
import * as _ from "lodash";

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
 */
export function createYTypeFromObject<T extends object>(obj: T): YType<T> {
  const objType = obj.constructor.name;
  if (objType === "String") {
    return new Y.Text(obj as unknown as string) as unknown as YType<T>;
  } else if (objType === "Array") {
    const yArray = new Y.Array();
    // Create YType for each array item and push
    for (const item of obj as any) {
      yArray.push([createYTypeFromObject(item) as unknown]);
    }
    return yArray as unknown as YType<T>;
  } else if (objType === "Object") {
    // return new
    const yMap = new Y.Map();
    Object.keys(obj).forEach((k: string) => {
      const value = obj[k as keyof T] as any;
      if (value !== undefined) {
        yMap.set(k, tryToCreateYTypeFromObject(value));
      }
    });
    return yMap as unknown as YType<T>;
  } else {
    throw TypeError(`Cannot create YType from ${objType}!`);
  }
}

/**
 * Given any type, creates a <code>YType</code> if it is an object, or return itself as-is if it is a primitive type.
 * @param obj
 */
export function tryToCreateYTypeFromObject(obj: any): any {
  try {
    return createYTypeFromObject(obj as unknown as object);
  } catch (e) {
    if (e instanceof TypeError) {
      return obj;
    }
  }
}

/**
 * Updates a <code>YType</code> in-place given a new <b>normal object</b> version of this <code>YType</code>.
 * @param oldYObj The old <code>YType</code> to be updated.
 * @param newObj The new normal object, must be the same template type as the <code>YType</code> to be updated.
 */
export function updateYTypeFromObject<T extends object>(oldYObj: YType<T>, newObj: T) {
  const newObjType = newObj.constructor.name;
  const oldObjType = oldYObj.toJSON().constructor.name;
  if (newObjType !== oldObjType) throw TypeError(`Type ${newObjType} cannot be used to update YType of ${oldObjType}`);
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
    const newArrLen = newObjAsArr.length;
    const oldObjAsArr = oldYObjAsYArray.toJSON();
    const oldArrLen = oldObjAsArr.length;
    for (let i = 0; i < newArrLen; i++) {
      if (i >= oldArrLen) {
        oldYObjAsYArray.push([tryToCreateYTypeFromObject(newObjAsArr[i])]);
      } else {
        if (!_.isEqual(oldObjAsArr[i], newObjAsArr[i])) {
          try {
            updateYTypeFromObject(oldYObjAsYArray.get(i), newObjAsArr[i]);
          } catch (e) {
            if (e instanceof TypeError) {
              oldYObjAsYArray.delete(i, 1);
              oldYObjAsYArray.insert(i, [tryToCreateYTypeFromObject(newObjAsArr[i])]);
            }
          }
        }
      }
    }
  } else if (newObjType === "Object") {
    const oldYObjAsYMap = oldYObj as unknown as Y.Map<any>;
    const oldObj = oldYObjAsYMap.toJSON() as T;
    Object.keys(newObj).forEach((k: string) => {
      const newValue = newObj[k as keyof T] as any;
      if (newValue) {
        if (!_.isEqual(oldObj[k as keyof T], newValue)) {
          try {
            updateYTypeFromObject(oldYObjAsYMap.get(k), newValue);
          } catch (e) {
            if (e instanceof TypeError) {
              oldYObjAsYMap.set(k, tryToCreateYTypeFromObject(newValue));
            }
          }
        }
        console.log(k, newValue, newObj, oldYObj.toJSON());
      }
    });
  } else {
    throw TypeError(`Cannot update YType on ${newObjType}!`);
  }
}
