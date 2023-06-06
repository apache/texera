import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { environment } from "../../../../../environments/environment";
import { OperatorSchema } from "../../../types/operator-schema.interface";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema.service";
import { SchemaAttribute, SchemaPropagationService } from "../schema-propagation/schema-propagation.service";

// By contract, property name name for texera table name autocomplete
export const tableNameInJsonSchema = "tableName";
export const fileNameInJsonSchema = "fileName";

/**
 * SourceTablesService contacts the backend API when the frontend starts up to fetch source table info.
 *
 * SourceTablesService transforms the Source operators which use Texera's internal tables,
 *  where the input box for table name is changed to a drop-down menu of available tables.
 * By contract, the attribute `tableName` is treated as a texera table.
 *
 * SourceTablesService also handles changing the `attribute` and `attributes` property of the source operators.
 * When a table is selected, then `attribute` or `attributes` of a source operator is also changed to a drop-down menu.
 * The schema propagation doesn't handle source operators because
 *  the result only contains the input property of each operator, but source operators don't have any input.
 *
 */
@Injectable({
  providedIn: "root",
})
export class SourceTablesService {
  // map of tableName and table's schema of all available source tables, undefined indicates they are unknown
  // example:
  // "table1": {attributes: [{attributeName: "attr1", attributeType: "string"}, {attributeName: "attr2", attributeType: "int"}] }
  private tableSchemaMap: Map<string, TableSchema> | undefined;
  private tableNames: string[] | undefined;
  private userFileNames: string[] | undefined;

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // do nothing if source tables are not enabled
    if (!environment.sourceTableEnabled) {
      return;
    }

    // when GUI starts up, fetch the source table information from the backend
    // this.registerSourceTableFetch(); // disabled as source tables are not used in the new engine

    this.registerOpPropertyDynamicUpdate();

    this.dynamicSchemaService.registerInitialSchemaTransformer((op, schema) => this.transformInitialSchema(op, schema));
  }

  /**
   * Retrieves the source tables in the system and their corresponding table schema.
   */
  public getTableSchemaMap(): ReadonlyMap<string, TableSchema> | undefined {
    return this.tableSchemaMap;
  }
  private changeInputToEnumInJsonSchema(
    schema: OperatorSchema,
    key: string,
    enumArray: string[] | undefined,
    title: string = ""
  ): OperatorSchema | undefined {
    if (!(schema.jsonSchema.properties && key in schema.jsonSchema.properties)) {
      return undefined;
    }

    let newDynamicSchema: OperatorSchema;
    if (enumArray && enumArray.length > 0) {
      newDynamicSchema = {
        ...schema,
        jsonSchema: DynamicSchemaService.mutateProperty(
          schema.jsonSchema,
          (k, v) => k === key,
          () => ({ type: "string", enum: enumArray, uniqueItems: true, title })
        ),
      };
    } else {
      newDynamicSchema = {
        ...schema,
        jsonSchema: DynamicSchemaService.mutateProperty(
          schema.jsonSchema,
          (k, v) => k === key,
          () => ({ type: "string", title })
        ),
      };
    }
    return newDynamicSchema;
  }

  /**
   * transform the initial schema to modify the `tableName` property from an input box to be a drop down menu.
   * This function will be registered as to DynamicSchemaService that triggers when a dynamic schema is first constructed.
   */
  private transformInitialSchema(operator: OperatorPredicate, schema: OperatorSchema): OperatorSchema {
    // change the tableName to a dropdown enum of available tables in the system
    const tableScanSchema = this.changeInputToEnumInJsonSchema(schema, tableNameInJsonSchema, this.tableNames);
    if (tableScanSchema) {
      return tableScanSchema;
    }
    const fileSchema = this.changeInputToEnumInJsonSchema(schema, fileNameInJsonSchema, this.userFileNames, "File");
    if (fileSchema) {
      return fileSchema;
    }
    return schema;
  }
  /**
   * Handle property change of source operators. When a table of a source operator is selected,
   *  and the source operator also has property `attribute` or `attributes`, change them to be the column names of the table.
   */
  private handlePropertyChange(operator: OperatorPredicate) {
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operator.operatorID);
    // for a source operator, change the attributes if a tableName has been chosen
    if (
      this.tableSchemaMap &&
      dynamicSchema.jsonSchema.properties &&
      tableNameInJsonSchema in dynamicSchema.jsonSchema.properties
    ) {
      const tableSchema = this.tableSchemaMap.get(operator.operatorProperties[tableNameInJsonSchema]);
      if (tableSchema) {
        const newDynamicSchema = SchemaPropagationService.setOperatorInputAttrs(dynamicSchema, [
          tableSchema.attributes,
        ]);
        this.dynamicSchemaService.setDynamicSchema(operator.operatorID, newDynamicSchema);
      }
    }
  }

  private registerOpPropertyDynamicUpdate(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPropertyChangeStream()
      .subscribe(event => this.handlePropertyChange(event.operator));
  }
}

export interface TableMetadata
  extends Readonly<{
    tableName: string;
    schema: TableSchema;
  }> {}

export interface TableSchema
  extends Readonly<{
    attributes: ReadonlyArray<SchemaAttribute>;
  }> {}
