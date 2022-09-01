import { Injectable } from "@angular/core";
import { JSONSchema7, JSONSchema7Definition } from "json-schema";
import { cloneDeep, isEqual } from "lodash-es";
import { Observable } from "rxjs";
import { Subject } from "rxjs";
import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { BreakpointSchema, OperatorPredicate } from "../../types/workflow-common.interface";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  OperatorInputSchema,
  SchemaAttribute,
  SchemaPropagationService,
} from "./schema-propagation/schema-propagation.service";

// @ts-ignore
import jsonRefLite from "json-ref-lite";
// @ts-ignore
import { levenshtein } from "edit-distance";

export type SchemaTransformer = (operator: OperatorPredicate, schema: OperatorSchema) => OperatorSchema;

/**
 * Dynamic Schema Service associates each operator with its own OperatorSchema,
 *  which could be different from the (static) schema of the operator type.
 *
 * Dynamic Schema of an operator can be changed through
 *  when an operator is first added, other modules can transform the initial schema by registering hook functions
 *  after an operator is added, modules, other modules can dynamically set the schema based on its need
 *
 * Currently, dynamic schema is changed through the following scenarios:
 *  - source table names autocomplete by SourceTablesService
 *  - attribute names autocomplete by SchemaPropagationService
 *
 */
@Injectable({
  providedIn: "root",
})
export class DynamicSchemaService {
  // dynamic schema of operators in the current workflow, specific to an operator and different from the static schema
  // directly calling `set()` is prohibited, it must go through `setDynamicSchema()`
  private dynamicSchemaMap = new Map<string, OperatorSchema>();

  // dynamic schema of link breakpoints in the current workflow
  private dynamicBreakpointSchemaMap = new Map<string, BreakpointSchema>();

  private initialSchemaTransformers: SchemaTransformer[] = [];

  // this stream is used to capture the event when the dynamic schema of an existing operator is changed
  private operatorDynamicSchemaChangedStream = new Subject<DynamicSchemaChange>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) // private schemaPropagationService: SchemaPropagationService,
  {
    // when an operator is added, add it to the dynamic schema map
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorAddStream()
      .subscribe(operator => {
        this.setDynamicSchema(operator.operatorID, this.getInitialDynamicSchema(operator));
      });

    // when an operator is deleted, remove it from the dynamic schema map
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDeleteStream()
      .subscribe(event => this.dynamicSchemaMap.delete(event.deletedOperator.operatorID));

    // when a link is deleted, remove it from the dynamic schema map
    this.workflowActionService
      .getTexeraGraph()
      .getLinkDeleteStream()
      .subscribe(event => this.dynamicBreakpointSchemaMap.delete(event.deletedLink.linkID));

    this.getOperatorDynamicSchemaChangedStream().subscribe(
      ({ operatorID, dynamicSchema, oldInputSchema, newInputSchema }) => {
        this.updateOperatorPropertiesOnSchemaChange(operatorID, dynamicSchema, oldInputSchema, newInputSchema);
      }
    );
  }

  private updateOperatorPropertiesOnSchemaChange(
    operatorID: string,
    dynamicSchema: OperatorSchema,
    oldInputSchema: OperatorInputSchema,
    newInputSchema: OperatorInputSchema
  ): void {
    console.log(dynamicSchema, oldInputSchema, newInputSchema);
    if (!dynamicSchema.jsonSchema.properties) return;
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    // console.log(operator)
    for (let port = 0; port < oldInputSchema.length; ++port) {
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
      const mapping: SchemaAttribute[][] = attributeMapping.pairs();
      const resolvedJsonSchema = jsonRefLite.resolve(dynamicSchema.jsonSchema);
      // console.log("resolved", resolvedJsonSchema)
      const newProperty = this.updateOperatorProperty(
        operator.operatorProperties,
        mapping,
        resolvedJsonSchema.properties,
        port
      );
      this.workflowActionService.setOperatorProperty(operatorID, newProperty);
      console.log("updated", this.workflowActionService.getTexeraGraph().getOperator(operatorID));
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
              if (!pair[0] || properties[key] === pair[0].attributeName) {
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
              if (updatePropertyRecurse(properties[key][index], (jsonSchemaProperty.items as CustomJSONSchema7).properties))
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

  /**
   * Register an hook function that transforms the *initial* dynamic schema when an operator is first added.
   * The SchemaTransformer is a function that takes the current schema and returns a new schema.
   *
   * Note: multiple transformers might be invoked when first constructing the initial schema,
   * transformers needs to be careful to not override other transformer's work.
   */
  public registerInitialSchemaTransformer(schemaTransformer: SchemaTransformer) {
    this.initialSchemaTransformers.push(schemaTransformer);
  }

  /**
   * Returns the observable which outputs the operatorID of which the dynamic schema has changed.
   */
  public getOperatorDynamicSchemaChangedStream(): Observable<DynamicSchemaChange> {
    return this.operatorDynamicSchemaChangedStream.asObservable();
  }

  /**
   * Returns the current dynamic schema of all operators.
   */
  public getDynamicSchemaMap(): ReadonlyMap<string, OperatorSchema> {
    return this.dynamicSchemaMap;
  }

  /**
   * Based on the operatorID, get the current dynamic operator schema that is created through autocomplete
   */
  public getDynamicSchema(operatorID: string): OperatorSchema {
    const dynamicSchema = this.dynamicSchemaMap.get(operatorID);
    if (!dynamicSchema) {
      throw new Error(`dynamic schema not found for ${operatorID}`);
    }
    return dynamicSchema;
  }

  /**
   * Based on the linkID, get the current link breakpoint schema
   * if there is no schema stored for a link, fetch the schema from
   * operatorMetadataService and set it in the map
   */
  public getDynamicBreakpointSchema(linkID: string): BreakpointSchema {
    if (!this.dynamicBreakpointSchemaMap.has(linkID)) {
      this.dynamicBreakpointSchemaMap.set(linkID, this.operatorMetadataService.getBreakpointSchema());
    }
    const dynamicBreakpointSchema = this.dynamicBreakpointSchemaMap.get(linkID);
    if (!dynamicBreakpointSchema) {
      throw new Error("dynamic breakpoint schema not found.");
    }
    return dynamicBreakpointSchema;
  }

  /**
   * Returns the current dynamic breakpoint schema of all links.
   */
  public getDynamicBreakpointSchemaMap(): ReadonlyMap<string, BreakpointSchema> {
    return this.dynamicBreakpointSchemaMap;
  }

  /**
   * Sets the dynamic schema of an operator. If the new schema is different, also emit dynamic schema changed event.
   *
   * The new dynamic schema is validated against the current operator properties.
   * If the changed new dynamic schema invalidates some property, then the invalid properties fields will be dropped.
   *
   */
  public setDynamicSchema(
    operatorID: string,
    dynamicSchema: OperatorSchema,
    oldInputSchema: OperatorInputSchema = [],
    newInputSchema: OperatorInputSchema = []
  ): void {
    const currentDynamicSchema = this.dynamicSchemaMap.get(operatorID);

    // do nothing if old & new schema are the same
    if (isEqual(currentDynamicSchema, dynamicSchema)) {
      return;
    }

    // set the new dynamic schema
    this.dynamicSchemaMap.set(operatorID, dynamicSchema);
    // only emit event if the old dynamic schema is not present
    if (currentDynamicSchema) {
      console.log("dynamic schema changed!");
      this.operatorDynamicSchemaChangedStream.next({
        operatorID: operatorID,
        dynamicSchema: dynamicSchema,
        oldInputSchema: oldInputSchema,
        newInputSchema: newInputSchema,
      });
    }
  }

  /**
   * Gets the initial dynamic schema of an operator type, which might be different from its static schema.
   * Currently, the only case is to change the source operators to have autocomplete of available tablenames.
   *
   * @param operator
   */
  private getInitialDynamicSchema(operator: OperatorPredicate): OperatorSchema {
    let initialSchema = this.operatorMetadataService.getOperatorSchema(operator.operatorType);
    this.initialSchemaTransformers.forEach(transformer => (initialSchema = transformer(operator, initialSchema)));

    return initialSchema;
  }

  /**
   * Helper function to change a property in a json schema of an operator schema.
   * It recursively walks through the property field of a JSON schema, and tries to find the property name.
   * Once it finds the property name, it invokes the mutationFunction to get the new property and replaces the old property.
   * The mutationFunction optionally takes a input with current property of the propertyName and outputs the new mutated property.
   *
   * Returns a new object containing the new json schema property.
   */
  public static mutateProperty(
    jsonSchemaToChange: CustomJSONSchema7,
    matchFunc: (propertyName: string, propertyValue: CustomJSONSchema7) => boolean,
    mutationFunc: (propertyValue: CustomJSONSchema7) => CustomJSONSchema7
  ): CustomJSONSchema7 {
    // recursively walks the JSON schema property tree to find the property name
    const mutatePropertyRecurse = (jsonSchema: JSONSchema7) => {
      const schemaProperties = jsonSchema.properties;
      const schemaDefinitions = jsonSchema.definitions;
      const schemaItems = jsonSchema.items;

      // nested JSON schema property can have 2 types: object or array
      const mutateObjectProperty = (objectProperty: { [key: string]: JSONSchema7Definition }) => {
        Object.entries(objectProperty).forEach(([propertyName, propertyValue]) => {
          if (typeof propertyValue === "boolean") {
            return;
          }
          if (matchFunc(propertyName, propertyValue as CustomJSONSchema7)) {
            objectProperty[propertyName] = mutationFunc(propertyValue as CustomJSONSchema7);
          } else {
            mutatePropertyRecurse(propertyValue);
          }
        });
      };
      const mutateArrayProperty = (arrayProperty: JSONSchema7Definition[]) => {
        arrayProperty.forEach(item => {
          if (typeof item !== "boolean") {
            mutatePropertyRecurse(item);
          }
        });
      };

      if (schemaProperties) {
        mutateObjectProperty(schemaProperties);
      }
      if (schemaDefinitions) {
        mutateObjectProperty(schemaDefinitions);
      }
      if (schemaItems && typeof schemaItems !== "boolean") {
        if (Array.isArray(schemaItems)) {
          mutateArrayProperty(schemaItems);
        } else {
          mutatePropertyRecurse(schemaItems);
        }
      }
    };

    // deep copy the schema first to avoid changing the original schema object
    const jsonSchemaCopy = cloneDeep(jsonSchemaToChange);
    mutatePropertyRecurse(jsonSchemaCopy);

    return jsonSchemaCopy;
  }
}

export interface DynamicSchemaChange {
  operatorID: string;
  dynamicSchema: OperatorSchema;
  oldInputSchema: OperatorInputSchema;
  newInputSchema: OperatorInputSchema;
}
