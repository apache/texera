import { JSONSchema7, JSONSchema7Definition } from 'json-schema';

export interface CustomJSONSchema7 extends JSONSchema7 {
  properties?: {
    [key: string]: CustomJSONSchema7 | boolean;
  };
  items?: CustomJSONSchema7 | boolean | JSONSchema7Definition[];

  // new custom properties:
  autofill?: 'attributeName' | 'attributeNameList';
  autofillAttributeOnPort?: number;
}
