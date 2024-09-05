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
        const length = value.length;
        // If length is less than 13, show the entire string.
        if (length < 13) {
          return `bytes'${value}' (length: ${length})`;
        }
        // Otherwise, show the leading and trailing bytes with ellipsis in between.
        const leadingBytes = value.slice(0, 10);
        // If the length of the value is less than 10, leadingBytes will take the entire string.
        const trailingBytes = value.slice(-3);
        // If the length of the value is less than 3, trailingBytes will take the entire string.
        return `bytes'${leadingBytes}...${trailingBytes}' (length: ${length})`;
      }
      if (value.length > maxLen) {
        return value.substring(0, maxLen) + "...";
      }
    }
    return value;
  });
}
