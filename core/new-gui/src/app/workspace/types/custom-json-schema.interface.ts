import { JSONSchema7, JSONSchema7Definition } from "json-schema";

export interface CustomJSONSchema7 extends JSONSchema7 {
  propertyOrder?: number;
  properties?: {
    [key: string]: CustomJSONSchema7 | boolean;
  };
  items?: CustomJSONSchema7 | boolean | JSONSchema7Definition[];

  // new custom properties:
  autofill?: "attributeName" | "attributeNameList";
  autofillAttributeOnPort?: number;

  "enable-presets"?: boolean; // include property in schema of preset

  dependOn?: string;
  toggleHidden?: string[]; // the field names which will be toggle hidden or not by this field.
}
