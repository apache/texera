import { Injectable } from '@angular/core';
import { JSONSchema4 } from 'json-schema';

import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import '../../../common/rxjs-operators';

import { OperatorPredicate } from '../../types/workflow-common.interface';
import { OperatorSchema } from '../../types/operator-schema.interface';

import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { isEqual, remove, cloneDeep, get, set } from 'lodash-es';
import * as Ajv from 'ajv';

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
  providedIn: 'root'
})
export class DynamicSchemaService {

  // dynamic schema of operators in the current workflow, specific to an operator and different from the static schema
  // directly calling `set()` is prohibited, it must go through `setDynamicSchema()`
  private dynamicSchemaMap = new Map<string, OperatorSchema>();

  private initialSchemaTransformers: SchemaTransformer[] = [];

  // this stream is used to capture the event when the dynamic schema of an existing operator is changed
  private operatorDynamicSchemaChangedStream = new Subject<{ operatorID: string }>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
    // when an operator is added, add it to the dynamic schema map
    this.workflowActionService.getTexeraGraph().getOperatorAddStream()
      .subscribe(operator => {
        this.setDynamicSchema(operator.operatorID, this.getInitialDynamicSchema(operator));
      });

    // when an operator is deleted, remove it from the dynamic schema map
    this.workflowActionService.getTexeraGraph().getOperatorDeleteStream()
      .subscribe(event => this.dynamicSchemaMap.delete(event.deletedOperator.operatorID));
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
  public getOperatorDynamicSchemaChangedStream(): Observable<{ operatorID: string }> {
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
   * Sets the dynamic schema of an operator. If the new schema is different, also emit dynamic schema changed event.
   *
   * The new dynamic schema is validated against the current operator properties.
   * If the changed new dynamic schema invalidates some property, then the invalid properties fields will be dropped.
   *
   */
  public setDynamicSchema(operatorID: string, dynamicSchema: OperatorSchema): void {
    const oldDynamicSchema = this.dynamicSchemaMap.get(operatorID);

    // do nothing if old & new schema are the same
    if (isEqual(oldDynamicSchema, dynamicSchema)) {
      return;
    }

    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (! operator) {
      throw new Error(`operator ${operatorID} not found`);
    }
    // if  dynamic schema is not set yet, use its static schema as initial dynamic schema
    let currentDynamicSchema: OperatorSchema;
    if (oldDynamicSchema) {
      currentDynamicSchema = oldDynamicSchema;
    } else {
      currentDynamicSchema = this.operatorMetadataService.getOperatorSchema(operator.operatorType);
    }

    // new dynamic schema might introduce additional errors for the current operator property data
    // check for new errors and if any exists, set the new errored property to undefined
    // validate the data against old and new schema, and diff newErrorList - oldErrorList

    const oldErrors = DynamicSchemaService.validateJsonSchema(operator, currentDynamicSchema);
    const newErrors = DynamicSchemaService.validateJsonSchema(operator, dynamicSchema);
    const errorsDiff = DynamicSchemaService.diffAjvErrors(oldErrors, newErrors);

    if (errorsDiff.length > 0) {
      const newOperatorProperty = cloneDeep(operator.operatorProperties);
      errorsDiff.forEach(error => {
        // error.dataPath is a string to access the property, starts with a dot, for example ".a.b.c"
        // lodash.set will set the property based on the string path, equiavlent to "obj.a.b.c = value"
        set(newOperatorProperty, error.dataPath.substr(1), undefined);
      });
      this.workflowActionService.setOperatorProperty(operator.operatorID, newOperatorProperty);
    }

    // set the new dynamic schema and emit event
    this.dynamicSchemaMap.set(operator.operatorID, dynamicSchema);
    if (oldDynamicSchema) {
      this.operatorDynamicSchemaChangedStream.next({ operatorID });
    }
  }

  /**
   * Gets the inital dynamic schema of an operator type, which might be different from its static schema.
   * Currently, the only case is to change the source operators to have autocomplete of available tablenames.
   *
   * @param operatorType
   */
  private getInitialDynamicSchema(operator: OperatorPredicate): OperatorSchema {
    const staticSchema = this.operatorMetadataService.getOperatorSchema(operator.operatorType);

    let initialSchema = staticSchema;
    this.initialSchemaTransformers.forEach(transformer => initialSchema = transformer(operator, initialSchema));

    return initialSchema;
  }


  /**
   * Helper function to change a property in a json schema of an operator schema.
   * Returns a new operator schema containing the new json schema property.
   */
  public static mutateProperty(
    jsonSchemaToChange: JSONSchema4, propertyName: string, mutationFunc: (arg: JSONSchema4) => JSONSchema4): JSONSchema4 {

    const mutatePropertyRecurse = (jsonSchema: JSONSchema4) => {
      const schemaProperties = jsonSchema.properties;
      const schemaItems = jsonSchema.items;
      if (schemaProperties) {
        Object.keys(schemaProperties).forEach(property => {
          if (property === propertyName) {
            schemaProperties[propertyName] = mutationFunc(schemaProperties[propertyName]);
          } else {
            mutatePropertyRecurse(schemaProperties[property]);
          }
        });
      }
      if (schemaItems) {
        if (Array.isArray(schemaItems)) {
          schemaItems.forEach(item => mutatePropertyRecurse(item));
        } else {
          mutatePropertyRecurse(schemaItems);
        }
      }
    };

    const jsonSchemaCopy = cloneDeep(jsonSchemaToChange);
    mutatePropertyRecurse(jsonSchemaCopy);

    return jsonSchemaCopy;
  }


  /**
   * This method uses `Another JSON Schema Validator` library to check if the data passed
   *  into the method satisfy the constraint set by the Json Schema for an operator
   * https://github.com/epoberezkin/ajv
   *
   * @param schema json schema of an operator
   * @param data data to check
   */
  private static validateJsonSchema(operator: OperatorPredicate, schema: OperatorSchema): Ajv.ErrorObject[] | undefined {
    const ajv = new Ajv({ schemaId: 'auto', allErrors: true });
    // only supports version 4 json schema currently
    ajv.addMetaSchema(require('ajv/lib/refs/json-schema-draft-04.json'));
    ajv.validate(schema.jsonSchema, operator.operatorProperties);
    return ajv.errors;
  }

  /**
   * Calcultes the diff between newErrors and oldErrors of ajv validation result.
   */
  private static diffAjvErrors(oldErrors: Ajv.ErrorObject[] | undefined, newErrors: Ajv.ErrorObject[] | undefined): Ajv.ErrorObject[] {
    if (!newErrors) {
      return [];
    }
    if (!oldErrors) {
      return newErrors;
    }
    const errorsDiff = remove(newErrors, error =>
      oldErrors.filter(e => isEqual(e, error)).length === 0
    );
    return errorsDiff;
  }

}
