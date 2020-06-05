import { isEqual } from 'lodash';
import { UserFileService } from '../../../../common/service/user/user-file/user-file.service';
import { AppSettings } from './../../../../common/app-setting';
import { environment } from '../../../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { OperatorSchema } from '../../../types/operator-schema.interface';

import { SchemaPropagationService } from '../schema-propagation/schema-propagation.service';
import { WorkflowActionService } from './../../workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from './../dynamic-schema.service';

// endpoint for retrieving table metadata
export const SOURCE_TABLE_NAMES_ENDPOINT = 'resources/table-metadata';
// By contract, property name name for texera table name autocomplete
export const tableNameInJsonSchema = 'tableName';
export const fileNameInJsonSchema = 'fileName';


/**
 * SourceTablesService contacts the backend API when the frontend starts up to fetch source table info.
 *
 * SourceTablesService transforms the Source operators which use Texera's internal tables,
 *  where the input box for table name is changed to a drop-down menu of available tables.
 * By contract, the attribute `tableName` is treated as a texera table.
 *
 * SourceTablesService also handles changing the `attribute` and `attributes` property of the source operators.
 * When a table is selected, then `attribute` or `attributes` of a source operator is also changed to a drop-down menu.
 * The schema propagation doesn't handle source operators becaue
 *  the result only contains the input property of each operator, but source operators don't have any input.
 *
 */
@Injectable({
  providedIn: 'root'
})
export class SourceTablesService {

  // map of tableName and table's schema of all available source tables, undefined indicates they are unknown
  // example:
  // "table1": {attributes: [{attrributeName: "attr1", attributeType: "string"}, {attrributeName: "attr2", attributeType: "int"}] }
  private tableSchemaMap: Map<string, TableSchema> | undefined;
  private userFiles: string[] | undefined;

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private userFileService: UserFileService
  ) {
    // do nothing if source tables are not enabled
    if (!environment.sourceTableEnabled) {
      return;
    }

    // when GUI starts up, fetch the source table information frmo the backend
    this.invokeSourceTableAPI().subscribe(
      response => this.tableSchemaMap = response
    );

    this.userFileService.getUserFilesChangedEvent().subscribe(
      event => {
        if (event) {
          this.userFiles = event.map(file => file.name);
        } else {
          this.userFiles = undefined;
        }
        this.handleUserFileChange();
      }
    );

    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream().subscribe(
      event => this.handlePropertyChange(event.operator)
    );
    this.dynamicSchemaService.registerInitialSchemaTransformer((op, schema) => this.transformInitialSchema(op, schema));
  }

  /**
   * Reterieves the source tables in the system and their corresponding table schema.
   */
  public getTableSchemaMap(): ReadonlyMap<string, TableSchema> | undefined {
    return this.tableSchemaMap;
  }

  private invokeSourceTableAPI(): Observable<Map<string, TableSchema>> {
    return this.httpClient
      .get<TableMetadata[]>(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`)
      .map(tableDetails => new Map(tableDetails.map(i => [i.tableName, i.schema] as [string, TableSchema])));
  }

  /**
   * transform the initial schema to modify the `tableName` property from an input box to be a drop down menu.
   * This function will be registered as to DynamicSchemaService that triggers when a dynamic schema is first constructed.
   */
  private transformInitialSchema(operator: OperatorPredicate, schema: OperatorSchema): OperatorSchema {
    // change the tableName to a dropdown enum of available tables in the system
    if (this.tableSchemaMap && schema.jsonSchema.properties && tableNameInJsonSchema in schema.jsonSchema.properties) {
      const tableNames = Array.from(this.tableSchemaMap.keys());
      return {
        ...schema,
        jsonSchema: DynamicSchemaService.mutateProperty(
          schema.jsonSchema, tableNameInJsonSchema, () => ({ type: 'string', enum: tableNames, uniqueItems: true}))
      };
    }
    // change the filename to a dropdown enum of available files of the user in the system
    if (this.userFiles && schema.jsonSchema.properties && fileNameInJsonSchema in schema.jsonSchema.properties) {
      const fileNames = this.userFiles.slice();
      return {
        ...schema,
        jsonSchema: DynamicSchemaService.mutateProperty(
          schema.jsonSchema, fileNameInJsonSchema, () => ({ type: 'string', enum: fileNames, uniqueItems: true}))
      };
    }
    return schema;
  }

  private handleUserFileChange() {
    Array.from(this.dynamicSchemaService.getDynamicSchemaMap().keys())
    .filter(operator => {
      const schema = this.dynamicSchemaService.getDynamicSchema(operator);
      return schema.jsonSchema.properties && fileNameInJsonSchema in schema.jsonSchema.properties;
    }).forEach(operatorID => {
      const schema = this.dynamicSchemaService.getDynamicSchema(operatorID);
      // if operator input attributes are in the result, set them in dynamic schema
      const fileNames = this.userFiles;
      let newDynamicSchema: OperatorSchema;
      if (fileNames) {
        newDynamicSchema = {
          ...schema,
          jsonSchema: DynamicSchemaService.mutateProperty(
            schema.jsonSchema, fileNameInJsonSchema, () => ({ type: 'string', enum: fileNames, uniqueItems: true }))
        };
      } else {
        newDynamicSchema = {
          ...schema,
          jsonSchema: DynamicSchemaService.mutateProperty(
            schema.jsonSchema, fileNameInJsonSchema, () => ({ type: 'string'}))
        };
      }

      if (! isEqual(schema, newDynamicSchema)) {
        SchemaPropagationService.resetAttributeOfOperator(this.workflowActionService, operatorID);
        this.dynamicSchemaService.setDynamicSchema(operatorID, newDynamicSchema);
      }

    });
  }

  /**
   * Handle property change of source operators. When a table of a source operator is selected,
   *  and the source operator also has property `attribute` or `attributes`, change them to be the column names of the table.
   */
  private handlePropertyChange(operator: OperatorPredicate) {
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operator.operatorID);
    // for a source operator, change the attributes if a tableName has been chosen
    if (this.tableSchemaMap && dynamicSchema.jsonSchema.properties && tableNameInJsonSchema in dynamicSchema.jsonSchema.properties) {
      const tableSchema = this.tableSchemaMap.get(operator.operatorProperties[tableNameInJsonSchema]);
      if (tableSchema) {
        const newDynamicSchema = SchemaPropagationService.setOperatorInputAttrs(
          dynamicSchema, tableSchema.attributes.map(attr => attr.attributeName));
        this.dynamicSchemaService.setDynamicSchema(operator.operatorID, newDynamicSchema);
      }
    }
  }

}

export interface TableMetadata extends Readonly <{
  tableName: string,
  schema: TableSchema
}> { }

export interface TableSchema extends Readonly<{
  attributes: ReadonlyArray<{
    attributeName: string,
    attributeType: string
  }>
}> { }
