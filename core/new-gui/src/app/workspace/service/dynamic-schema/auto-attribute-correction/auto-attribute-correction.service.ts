import { Injectable } from "@angular/core";
import {
  OperatorInputSchema,
  SchemaAttribute,
  SchemaPropagationService,
} from "../schema-propagation/schema-propagation.service";
import { DynamicSchemaService } from "../dynamic-schema.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { CustomJSONSchema7 } from "src/app/workspace/types/custom-json-schema.interface";
import { cloneDeep, isEqual } from "lodash-es";
// @ts-ignore TODO: Add typing for the below package
import { levenshtein } from "edit-distance";
import { Resolver } from "@stoplight/json-ref-resolver";
import { isNotNull, isNull } from "src/app/common/util/assert";
import { environment } from "src/environments/environment";

/**
 * Attribute Change Propagation Service propagates the change (adding, removing, and renaming) of attributes.
 *
 * - When user renames an attribute through an operator,
 * the attribute name will be updated in all succeeding operators.
 * - When user deletes an attribute (e.g., deselected in a projection operator),
 * all succeeding operators containing the attribute will delete the attribute from themselves and become invalid.
 */
@Injectable({
  providedIn: "root",
})
export class AutoAttributeCorrectionService {
  // keep a copy of input schema map,
  // the inputSchemaMap in SchemaPropagationService always keeps the input schema up-to-date
  // this map keeps the last **known** schema for each operator
  // if the current input schema is changed to undefined, this map still keeps the last known schema
  private lastOperatorInputSchema: Record<string, OperatorInputSchema> = {};

  constructor(
    private dynamicSchemaService: DynamicSchemaService,
    private schemaPropagationService: SchemaPropagationService,
    private workflowActionService: WorkflowActionService
  ) {
    if (environment.autoAttributeCorrectionEnabled) {
      this.registerInputSchemaChangedStream();
    }
  }

  private registerInputSchemaChangedStream() {
    this.schemaPropagationService.getOperatorInputSchemaChangedStream().subscribe(() => {
      Object.keys(this.schemaPropagationService.getOperatorInputSchemaMap()).forEach(op => {
        const currentSchema = this.schemaPropagationService.getOperatorInputSchema(op);
        if (currentSchema === undefined) {
          // intentionally skip updating unknown new schema
          // preserve the last known schema for comparing with the new schema
          return;
        }
        const oldSchema = this.lastOperatorInputSchema[op];
        this.lastOperatorInputSchema[op] = currentSchema;
        if (!isEqual(oldSchema, currentSchema) && oldSchema !== undefined) {
          this.updateOperatorPropertiesOnInputSchemaChange(op, oldSchema, currentSchema);
        }
      });
    });
  }

  private async updateOperatorPropertiesOnInputSchemaChange(
    operatorID: string,
    oldInputSchema: OperatorInputSchema,
    newInputSchema: OperatorInputSchema
  ): Promise<void> {
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
    if (!dynamicSchema.jsonSchema.properties) {
      return;
    }
    for (let port = 0; port < oldInputSchema.length; ++port) {
      const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
      const attributeMapping = levenshtein(
        oldInputSchema[port],
        newInputSchema[port],
        (_: any) => 1,
        (_: any) => 1,
        (a: SchemaAttribute, b: SchemaAttribute) => {
          if (a.attributeName === b.attributeName && a.attributeType === b.attributeType) return 0;
          else if (a.attributeName !== a.attributeName && a.attributeType !== b.attributeType) return 2;
          else return 1;
        }
      );
      // [[oldAttribute1, newAttribute1], [oldAttribute2, newAttribute2], ...]
      const mapping: SchemaAttribute[][] = attributeMapping.pairs();
      // Resolve #ref in some json schema (e.g. Filter)
      const resolvedJsonSchema = await new Resolver().resolve(dynamicSchema.jsonSchema);

      const matchFunc = (schema: CustomJSONSchema7, _: any) => {
        if (schema.autofill === undefined) {
          return false;
        }
        if (schema.autofillAttributeOnPort === undefined && port === 0) {
          return true;
        }
        return schema.autofillAttributeOnPort === port;
      };
      const mutationFunc = (schema: CustomJSONSchema7, value: any) => {
        if (schema.autofill === "attributeName") {
          // value must be a string
          const attrMapping = mapping.find(p => p[0].attributeName === value);
          if (attrMapping === undefined) {
            return value;
          }
          if (isNull(attrMapping[1])) {
            return "";
          }
          return attrMapping[1].attributeName;
        }
        if (schema.autofill === "attributeNameList") {
          // value must be a list of strings
          // map any existing variables
          const newAttrs = (value as any[])
            .map(attr => {
              const attrMapping = mapping.find(p => p[0].attributeName === value);
              if (attrMapping === undefined) {
                return value;
              }
              if (isNull(attrMapping[1])) {
                return "";
              }
              return attrMapping[1].attributeName;
            })
            .filter(v => !isNotNull(v) && v !== "");
          // depending on if this is a projection operator, add new values
          if (operator.operatorType === "Projection") {
            mapping.filter(p => isNotNull(p[0])).forEach(p => newAttrs.push(p[1]));
          }
        }
        return value;
      };

      const newProperty = this.updateOperatorProperty(
        operator.operatorProperties,
        resolvedJsonSchema.result,
        matchFunc,
        mutationFunc
      );
      if (!isEqual(newProperty, operator.operatorProperties)) {
        this.workflowActionService.setOperatorProperty(operatorID, newProperty);
      }
    }
  }

  private updateOperatorProperty(
    currentProperties: any,
    operatorJsonSchema: CustomJSONSchema7,
    matchFunc: (propertyValue: CustomJSONSchema7, value: any) => boolean,
    mutationFunc: (schema: CustomJSONSchema7, value: any) => any
  ): any {
    console.log(operatorJsonSchema);
    const updatePropertyRecurse = (value: any, jsonSchema: CustomJSONSchema7): any => {
      if (matchFunc(jsonSchema, value)) {
        return mutationFunc(jsonSchema, value);
      }

      if (jsonSchema.type === "array") {
        // value should also be an array,
        // with each element follow the schema of jsonSchemaProperty.items
        const childSchema = jsonSchema.items;
        if (childSchema === undefined) {
          return value;
        }
        const newValue: any[] = [];
        (value as any[]).forEach(element => {
          const newElement = updatePropertyRecurse(element, childSchema as CustomJSONSchema7);
          if (isNotNull(newElement)) {
            newValue.push(newElement);
          }
        });
        return newValue;
      } else if (jsonSchema.type === "object") {
        // value should be an object
        // with each key being string, each value follow the schema of each property
        const childSchema = jsonSchema.properties;
        if (childSchema === undefined) {
          return value;
        }
        const newValue: Record<string, any> = {};
        Object.keys(value as Record<string, any>).forEach(k => {
          const newElement = updatePropertyRecurse(value[k], childSchema[k] as CustomJSONSchema7);
          if (isNotNull(newElement)) {
            newValue[k] = newElement;
          }
        });
        return newValue;
      } else {
        // value should be a primitive object (string, int, boolean, etc..)
        if (matchFunc(jsonSchema, value)) {
          return mutationFunc(jsonSchema, value);
        } else {
          return value;
        }
      }
    };

    const valueCopy: any = cloneDeep(currentProperties);
    return updatePropertyRecurse(valueCopy, operatorJsonSchema);
  }
}
