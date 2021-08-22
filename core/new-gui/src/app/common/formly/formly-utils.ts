import { FormlyFieldConfig } from '@ngx-formly/core';
import { isDefined } from "../util/predicate";
import { SchemaAttribute } from "../../workspace/service/dynamic-schema/schema-propagation/schema-propagation.service";

export function getFieldByName(fieldName: string, fields: FormlyFieldConfig[])
  : FormlyFieldConfig | undefined {
  return fields.filter((field, _, __) => field.key === fieldName)[0];
}

export function setHideExpression(toggleHidden: string[], fields: FormlyFieldConfig[], hiddenBy: string): void {

  toggleHidden.forEach((hiddenFieldName) => {
    const fieldToBeHidden = getFieldByName(hiddenFieldName, fields);
    if (isDefined(fieldToBeHidden)) {
      fieldToBeHidden.hideExpression = '!model.' + hiddenBy;
    }
  });

}

export function setChildTypeDependency(attributes: ReadonlyArray<ReadonlyArray<SchemaAttribute> | null> | undefined, parentName: string,
                                       fields: FormlyFieldConfig[], childName: string): void {
  const timestampFieldNames = attributes?.flat().filter((attribute) => {
    return attribute.attributeType === 'timestamp';
  }).map(attribute => attribute.attributeName);

  if (timestampFieldNames) {
    const childField = getFieldByName(childName, fields);
    if (isDefined(childField)) {
      childField.expressionProperties = {
        // 'type': 'string',
        // 'templateOptions.type': JSON.stringify(timestampFieldNames) + '.includes(model.' + parentName + ')? \'string\' : \'number\'',

        'templateOptions.description': JSON.stringify(timestampFieldNames) + '.includes(model.' + parentName
          + ')? \'Input a datetime string\' : \'Input a positive number\''
      };
    }
  }

}
