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
import {
  AttributeType,
  SchemaAttribute,
} from "../../workspace/service/dynamic-schema/schema-propagation/schema-propagation.service";

export function formatBinaryData(value: string): string {
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

export function trimAndFormatData(value: any, attributeType: AttributeType, maxLen: number): string {
  if (value === null) {
    return "NULL";
  }
  if (attributeType === "binary") {
    return formatBinaryData(value);
  }
  if (attributeType === "string") {
    if (value.length > maxLen) {
      return value.substring(0, maxLen) + "...";
    }
  }
  return value?.toString() ?? "";
}

export function trimDisplayJsonData(
  rowData: IndexableObject,
  schema: ReadonlyArray<SchemaAttribute>,
  maxLen: number
): Record<string, unknown> {
  return deepMap<Record<string, unknown>>(rowData, (value, key) => {
    const attributeType = schema.find(attr => attr.attributeName === key)?.attributeType ?? "string";
    return trimAndFormatData(value, attributeType, maxLen);
  });
}
