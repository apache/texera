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

/**
 * TODO
 */
@Injectable({
  providedIn: 'root'
})
export class SourceTablesService {

  // map of tableName and table's schema of all available source tables, undefined indicates they are unknown
  // example:
  // "table1": {attributes: [{attrributeName: "attr1", attributeType: "string"}, {attrributeName: "attr2", attributeType: "int"}] }
  private tableSchemaMap: Map<string, TableSchema> | undefined;

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // do nothing if source tables are not enabled
    if (!environment.sourceTableEnabled) {
      return;
    }

    this.invokeSourceTableAPI().subscribe(
      response => { this.tableSchemaMap = response; }
    );
    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream().subscribe(
      event => this.handlePropertyChange(event.operator)
    );
    this.dynamicSchemaService.registerInitialSchemaTransformer((op, schema) => this.transformInitialSchema(op, schema));
  }

  public getTableSchemaMap(): ReadonlyMap<string, TableSchema> | undefined {
    return this.tableSchemaMap;
  }

  private invokeSourceTableAPI(): Observable<Map<string, TableSchema>> {
    return this.httpClient
      .get<SourceTableResponse>(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`)
      .filter(response => response.code === 0)
      .map(response => JSON.parse(response.message) as ReadonlyArray<SourceTableDetail>)
      .map(tableDetails => new Map(tableDetails.map(i => [i.tableName, i.schema] as [string, TableSchema])));
  }

  private handlePropertyChange(operator: OperatorPredicate) {
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operator.operatorID);
    // for a source operator, change the attributes if a tableName has been chosen
    if (this.tableSchemaMap && dynamicSchema.jsonSchema.properties && 'tableName' in dynamicSchema.jsonSchema.properties) {
      const tableSchema = this.tableSchemaMap.get(operator.operatorProperties['tableName']);
      if (tableSchema) {
        const newDynamicSchema = SchemaPropagationService.setOperatorInputAttrs(
          dynamicSchema, tableSchema.attributes.map(attr => attr.attributeName));
        this.dynamicSchemaService.setDynamicSchema(operator.operatorID, newDynamicSchema);
      }
    }
  }

  private transformInitialSchema(operator: OperatorPredicate, schema: OperatorSchema): OperatorSchema {
    // change the tableName to a dropdown enum of available tables in the system
    if (this.tableSchemaMap && schema.jsonSchema.properties && 'tableName' in schema.jsonSchema.properties) {
      const tableNames = Array.from(this.tableSchemaMap.keys());
      return {
        ...schema,
        jsonSchema: DynamicSchemaService.mutateProperty(
          schema.jsonSchema, 'tableName', () => ({ type: 'string', enum: tableNames }))
      };
    }
    return schema;
  }

}

/**
 * The type decalaration of the response sent by **backend** when
 * asking for source table names.
 */
export interface SourceTableResponse extends Readonly < {
  code: number,
  message: string
} > { }

export interface SourceTableDetail extends Readonly <{
  tableName: string,
  schema: TableSchema
}> { }

export interface TableSchema extends Readonly<{
  attributes: ReadonlyArray<{
    attributeName: string,
    attributeType: string
  }>
}> { }
