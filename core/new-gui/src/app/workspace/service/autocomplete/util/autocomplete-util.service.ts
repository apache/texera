import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { SourceTableNamesAPIResponse } from '../../../types/source-table-names.interface';
import { SourceTableDetails } from '../../../types/source-table-names.interface';

import cloneDeep from 'lodash-es/cloneDeep';

export const SOURCE_OPERATORS_REQUIRING_TABLENAMES: ReadonlyArray<string> = ['KeywordSource', 'RegexSource', 'WordCountIndexSource',
                                                                            'DictionarySource', 'FuzzyTokenSource', 'ScanSource'];

export class AutocompleteUtils {

  constructor() { }

  /**
  * This function takes the response from the table-metadata API and creates a list of tableNames
  * out of it.
  * @param response The response from resourse/table-metadata API
  */
  public static processSourceTableAPIResponse(response: SourceTableNamesAPIResponse): Array<string> {
    const message = response.message;
    const tablesList: ReadonlyArray<SourceTableDetails> = JSON.parse(message);
    const tableNames: Array<string> = [];
    tablesList.forEach((table) => {
      tableNames.push(table.tableName);
    });
    return tableNames;
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
    if (!tablesNames) {
      return operatorMetadata;
    }

    const operatorSchemaList: Array<OperatorSchema> = cloneDeep(operatorMetadata.operators.slice());
    for (let i = 0; i < operatorSchemaList.length; i++) {
      if (SOURCE_OPERATORS_REQUIRING_TABLENAMES.includes(operatorSchemaList[i].operatorType)) {
        const jsonSchemaToModify = cloneDeep(operatorSchemaList[i].jsonSchema);
        const operatorProperties = jsonSchemaToModify.properties;
        if (!operatorProperties) {
          throw new Error(`Operator ${operatorSchemaList[i].operatorType} does not have properties in its schema`);
        }

        operatorProperties['tableName'] = { type: 'string', enum: tablesNames.slice() };

        const newOperatorSchema: OperatorSchema = {
          ...operatorSchemaList[i],
          jsonSchema: jsonSchemaToModify
        };

        operatorSchemaList[i] = newOperatorSchema;
      }
    }

    const operatorMetadataModified: OperatorMetadata = {
      ...operatorMetadata,
      operators: operatorSchemaList
    };

    return operatorMetadataModified;
  }
}
