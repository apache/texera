/**
 * This method will recursively iterate through the content of the row data and shorten
 *  the column string if it exceeds a limit that will excessively slow down the rendering time
 *  of the UI.
 *
 * This method will return a new copy of the row data that will be displayed on the UI.
 *
 * @param rowData original row data returns from execution
 */
import { IndexableObject } from "../../workspace/types/result-table.interface";
import validator from "validator";
import deepMap from "deep-map";

function isBase64(str: string): boolean {
  return validator.isBase64(str);
}

function isBinary(str: string): boolean {
  const binaryRegex = /^[01]+$/;
  return binaryRegex.test(str);
}

export function trimDisplayJsonData(rowData: IndexableObject, maxLen: number): Record<string, unknown> {
  return deepMap<Record<string, unknown>>(rowData, value => {
    if (typeof value === "string") {
      if (isBase64(value) || isBinary(value)) {
        return `bytes<${value.slice(0, 3)}...${value.slice(-3)}>`;
      }
      if (value.length > maxLen) {
        return value.substring(0, maxLen) + "...";
      }
    }
    return value;
  });
}
