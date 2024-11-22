import { Injectable } from "@angular/core";
import { JSONSchema7, JSONSchema7Definition } from "json-schema";
import { cloneDeep, isEqual } from "lodash-es";
import { Observable } from "rxjs";
import { Subject } from "rxjs";
import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";

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
 *  - attribute names autocomplete by WorkflowCompilingService
 *
 */
@Injectable({
  providedIn: "root",
})
export class DynamicSchemaService {
  // dynamic schema of operators in the current workflow, specific to an operator and different from the static schema
  // directly calling `set()` is prohibited, it must go through `setDynamicSchema()`
  private dynamicSchemaMap = new Map<string, OperatorSchema>();

  private initialSchemaTransformers: SchemaTransformer[] = [];

  // this stream is used to capture the event when the dynamic schema of an existing operator is changed
  private operatorDynamicSchemaChangedStream = new Subject<{
    operatorID: string;
  }>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
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
      .subscribe(event => this.dynamicSchemaMap.delete(event.deletedOperatorID));
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
  public getOperatorDynamicSchemaChangedStream(): Observable<{
    operatorID: string;
  }> {
    return this.operatorDynamicSchemaChangedStream.asObservable();
  }

  /**
   * Returns the current dynamic schema of all operators.
   */
  public getDynamicSchemaMap(): ReadonlyMap<string, OperatorSchema> {
    return this.dynamicSchemaMap;
  }

  public dynamicSchemaExists(operatorID: string): boolean {
    return this.dynamicSchemaMap.has(operatorID);
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
   * Sets the dynamic schema of an operator. If the new schema is different, also emit dynamic schema changed event.
   *
   * The new dynamic schema is validated against the current operator properties.
   * If the changed new dynamic schema invalidates some property, then the invalid properties fields will be dropped.
   *
   */
  public setDynamicSchema(operatorID: string, dynamicSchema: OperatorSchema): void {
    const currentDynamicSchema = this.dynamicSchemaMap.get(operatorID);

    // do nothing if old & new schema are the same
    if (isEqual(currentDynamicSchema, dynamicSchema)) {
      return;
    }

    // set the new dynamic schema
    this.dynamicSchemaMap.set(operatorID, dynamicSchema);
    this.operatorDynamicSchemaChangedStream.next({ operatorID });
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
    mutationFunc: (propertyName: string, propertyValue: CustomJSONSchema7) => CustomJSONSchema7
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
            objectProperty[propertyName] = mutationFunc(propertyName, propertyValue as CustomJSONSchema7);
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
