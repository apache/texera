import { Injectable } from "@angular/core";
import {
  OperatorInputSchema,
  SchemaAttribute,
  SchemaPropagationService,
} from "../schema-propagation/schema-propagation.service";
import { DynamicSchemaService } from "../dynamic-schema.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { CustomJSONSchema7 } from "src/app/workspace/types/custom-json-schema.interface";
import { cloneDeep } from "lodash-es";
// TODO: Add typing for the below two packages
// @ts-ignore
import jsonRefLite from "json-ref-lite";
// @ts-ignore
import { levenshtein } from "edit-distance";

/**
 * Input Schema Propagation Service propagates the change (adding, removing, and renaming) of attributes.
 * 
 * - When user renames an attribute through an operator, 
 * the attribute name will be updated in all succeeding operators. 
 * - When user deletes an attribute (e.g., deselected in a projection operator), 
 * all succeeding operators containing the attribute will delete the attribute from themselves and become invalid. 
 * - Specifically, when a new attribute is added to the input schema of a projection operator, 
 * it will include the new attribute in its projection list.
 */
@Injectable({
  providedIn: "root",
})
export class InputSchemaPropagationService {
  // TODO: solve the problem that contructor is not called unless the service is created somewhere
  constructor(
    private dynamicSchemaService: DynamicSchemaService,
    private schemaPropagationService: SchemaPropagationService,
    private workflowActionService: WorkflowActionService
  ) {
    this.registerInputSchemaChangedStream();
  }

  private registerInputSchemaChangedStream() {
    this.schemaPropagationService
      .getOperatorInputSchemaChangedStream()
      .subscribe(({ operatorID, oldInputSchema, newInputSchema }) => {
        this.updateOperatorPropertiesOnInputSchemaChange(operatorID, oldInputSchema, newInputSchema);
      });
  }

  private updateOperatorPropertiesOnInputSchemaChange(
    operatorID: string,
    oldInputSchema: OperatorInputSchema,
    newInputSchema: OperatorInputSchema
  ): void {
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
    // console.log(dynamicSchema, oldInputSchema, newInputSchema);
    if (!dynamicSchema.jsonSchema.properties) return;
    if (!oldInputSchema) return;
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    // console.log(operator)
    for (let port = 0; port < oldInputSchema.length; ++port) {
      // TODO: the substitute cost function can be improved to reach expected behaviour
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
      const resolvedJsonSchema = jsonRefLite.resolve(dynamicSchema.jsonSchema);
      // console.log("resolved", resolvedJsonSchema)
      const newProperty = this.updateOperatorProperty(
        operator.operatorProperties,
        mapping,
        resolvedJsonSchema.properties,
        port
      );
      this.workflowActionService.setOperatorProperty(operatorID, newProperty);
      // console.log("updated", this.workflowActionService.getTexeraGraph().getOperator(operatorID));
    }
  }

  private updateOperatorProperty(
    currentProperties: Readonly<{ [key: string]: any }>,
    mapping: SchemaAttribute[][],
    jsonSchemaProperties: { [key: string]: boolean | CustomJSONSchema7 },
    port: number
  ) {
    // console.log("properties: ", currentProperties);
    // console.log("mapping: ", mapping);
    const updatePropertyRecurse = (
      properties: any,
      jsonSchema: { [key: string]: CustomJSONSchema7 | boolean } | undefined
    ): boolean => {
      // console.log("recurse:", properties, jsonSchema)
      if (!jsonSchema) return false;
      for (const key in jsonSchema) {
        if (typeof jsonSchema[key] === "boolean") continue;
        const jsonSchemaProperty = jsonSchema[key] as CustomJSONSchema7;
        if (jsonSchemaProperty.autofillAttributeOnPort === port) {
          if (jsonSchemaProperty.type === "array") {
            // TODO: If the operator is Projection,
            // then add new attributes to the list

            let newArray = [];
            // console.log(properties);
            // TODO: low efficiency, can be improved, e.g. by using map
            for (var i = 0; i < properties[key].length; ++i) {
              for (var j = 0; j < mapping.length; ++j) {
                const pair = mapping[j];
                // console.log(properties[key][i], pair)
                if (!pair[0] || (properties[key][i] === pair[0].attributeName && pair[1])) {
                  newArray.push(pair[1].attributeName);
                }
              }
            }
            // console.log("newArray", newArray)
            properties[key] = newArray;
          } else if ((jsonSchema[key] as CustomJSONSchema7).type === "string") {
            for (var i = 0; i < mapping.length; ++i) {
              const pair = mapping[i];
              if (pair[0] && properties[key] === pair[0].attributeName) {
                if (pair[1]) properties[key] = pair[1].attributeName;
                else return false; // the attribute was deleted
              }
            }
          }
        } else {
          if ((jsonSchema[key] as CustomJSONSchema7).type === "object") {
            updatePropertyRecurse(properties[key], jsonSchemaProperty.properties);
          } else if ((jsonSchema[key] as CustomJSONSchema7).type === "array") {
            let newArray = [];
            for (const index in properties[key]) {
              // console.log("items", prop.items);
              if (
                updatePropertyRecurse(
                  properties[key][index],
                  (jsonSchemaProperty.items as CustomJSONSchema7).properties
                )
              )
                newArray.push(properties[key][index]);
            }
            properties[key] = newArray;
          }
        }
      }
      return true;
    };
    const newProperties: { [key: string]: any } = cloneDeep(currentProperties);
    updatePropertyRecurse(newProperties, jsonSchemaProperties);
    // console.log("after recurse", newProperties)
    return newProperties;
  }
}
