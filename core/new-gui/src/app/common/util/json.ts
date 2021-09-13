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
import deepMap from "deep-map";

export function trimDisplayJsonData(rowData: IndexableObject, maxLen: number): Record<string, unknown> {
  return deepMap<Record<string, unknown>>(rowData, value => {
    if (typeof value === "string" && value.length > maxLen) {
      return value.substring(0, maxLen) + "...";
    } else {
      return value;
    }
  });
}
