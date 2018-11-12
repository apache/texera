import { HttpErrorResponse } from '@angular/common/http';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { SourceTableNamesAPIResponse } from '../../../types/autocomplete.interface';
import { SourceTableDetails, AutocompleteErrorResult } from '../../../types/autocomplete.interface';

import { WorkflowGraphReadonly } from '../../workflow-graph/model/workflow-graph';
import {
  LogicalLink, LogicalPlan, LogicalOperator,
} from '../../../types/execute-workflow.interface';

import cloneDeep from 'lodash-es/cloneDeep';

export class AutocompleteUtils {

  constructor() { }

  /**
  * This function takes the response from the table-metadata API and creates a list of tableNames
  * out of it.
  * @param response The response from resourse/table-metadata API
  */
  public static processSourceTableAPIResponse(response: SourceTableNamesAPIResponse): Array<string> {
    const tableNames: Array<string> = [];
    if (response.code !== 0) {
      return tableNames;
    }
    const message = response.message;
    const tablesList: ReadonlyArray<SourceTableDetails> = JSON.parse(message);
    return tablesList.map(table => table.tableName);
  }

  /**
   * This function takes the operator metadata returned by operator metadata service and modifies the
   *  schema of source operators which need a table name. Initially the operator schema had a property
   * called tableName which was just a string. After modification, the property tableName stays a string
   *  but takes enum as input. The values of the enum are the different table names which are available
   * at the server side.
   * @param operatorMetadata The metadata from operator metadata service
   */
  public static addSourceTableNamesToMetadata(operatorMetadata: OperatorMetadata,
    tablesNames: ReadonlyArray<string> | undefined): OperatorMetadata {
    // If the tableNames array is empty, just return the original operator metadata.
    if (!tablesNames || tablesNames.length === 0) {
      return operatorMetadata;
    }

    const operatorSchemaList: Array<OperatorSchema> = operatorMetadata.operators.map(
      operator => {
        const jsonSchemaToModify = cloneDeep(operator.jsonSchema);
        const operatorProperties = jsonSchemaToModify.properties;
        if (!operatorProperties) {
          throw new Error(`Operator ${operator.operatorType} does not have properties in its schema`);
        }
        // if the property contains a `tableName`, add the tableNames options to their json schema properties
        //  and return a new Operator Schema.
        if (operatorProperties['tableName']) {
          operatorProperties['tableName'] = { type: 'string', enum: tablesNames.slice() };

          const newOperatorSchema: OperatorSchema = {
            ...operator,
            jsonSchema: jsonSchemaToModify
          };

          return newOperatorSchema;
        }
        return operator;
      }
    );

    const operatorMetadataModified: OperatorMetadata = {
      ...operatorMetadata,
      operators: operatorSchemaList
    };

    return operatorMetadataModified;
  }

    /**
   * Modifies the schema of the operator according to autocomplete information obtained from the backend.
   * @param operatorSchema operator schema for the operator without autocomplete info filled in
   * @param inputSchema the input schema of the operator as inferred by autocomplete API
   */
  public static addInputSchemaToOperatorSchema(operatorSchema: OperatorSchema, inputSchema: ReadonlyArray<string>): OperatorSchema {
    // If the inputSchema is empty, just return the original operator metadata.
    if (!inputSchema || inputSchema.length === 0) {
      return operatorSchema;
    }

    const jsonSchemaToModify = cloneDeep(operatorSchema.jsonSchema);
    const operatorProperties = jsonSchemaToModify.properties;
    if (!operatorProperties) {
      throw new Error(`Operator ${operatorSchema.operatorType} does not have properties in its schema`);
    }

    // There are some operators which only take one of the attributes as input (eg: Analytics group) and some
    // which take more than one (eg: Search group). Therefore, the jsonSchema for these operators are different.
    // TODO: Standardize this i.e. All operators should accept input schema in a common key of json.
    // TODO: Join operators have two inputs - inner and outer. Autocomplete API currently returns all attributes
    //       in a single array. So, we can't differentiate between inner and outer. Therefore, autocomplete isn't applicable
    //       to Join yet.
    const items_key_in_schemajson = 'items';
    const attribute_list_key_in_schemajson = 'attributes';
    const single_attribute_in_schemajson = 'attribute';

    if (single_attribute_in_schemajson in operatorProperties) {
      operatorProperties[single_attribute_in_schemajson] = { type: 'string', enum: inputSchema.slice() };
    } else if (attribute_list_key_in_schemajson in operatorProperties
                  && items_key_in_schemajson in operatorProperties[attribute_list_key_in_schemajson]) {
      operatorProperties[attribute_list_key_in_schemajson][items_key_in_schemajson] = { type: 'string', enum: inputSchema.slice() };
    }

    const newOperatorSchema: OperatorSchema = {
      ...operatorSchema,
      jsonSchema: jsonSchemaToModify
    };

    return newOperatorSchema;
  }

    /**
   * Handles the HTTP Error response in different failure scenarios
   *  and converts to an ErrorExecutionResult object.
   * @param errorResponse
   */
  public static processErrorResponse(errorResponse: HttpErrorResponse): AutocompleteErrorResult {
    // client side error, such as no internet connection
    if (errorResponse.error instanceof ProgressEvent) {
      return {
        code: -1,
        message: 'Could not reach Texera server'
      };
    }

    // other kinds of server error
    return {
      code: -1,
      message: `Texera server autocomplete API error: ${errorResponse.error.message}`
    };
  }
}
