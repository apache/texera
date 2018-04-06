import { OperatorSchema, OperatorMetadata, GroupInfo } from '../../types/operator-schema';

/**
 * MOCK_OPERATOR_METADATA const variable is a mock operator metadata consisting of
 *  a few operators, which can be useful for testing.
 *
 */

export const MOCK_OPERATOR_SCHEMA_LIST: OperatorSchema[] = [
    {
        'operatorType': 'ScanSource',
        'additionalMetadata': {
            'advancedOptions': [],
            'operatorDescription': 'Read records from a table one by one',
            'operatorGroupName': 'Source',
            'numInputPorts': 0,
            'numOutputPorts': 1,
            'userFriendlyName': 'Source: Scan'
        },
        'jsonSchema': {
            'id': 'urn:jsonschema:edu:uci:ics:texera:dataflow:source:scan:ScanSourcePredicate',
            'properties': {
                'tableName': {
                    'type': 'string'
                }
            },
            'required': [
                'tableName'
            ],
            'type': 'object'
        }
    },
    {
        'operatorType': 'NlpSentiment',
        'additionalMetadata': {
            'advancedOptions': [],
            'operatorDescription': 'Sentiment analysis based on Stanford NLP package',
            'operatorGroupName': 'Analysis',
            'numInputPorts': 1,
            'numOutputPorts': 1,
            'userFriendlyName': 'Sentiment Analysis'
        },
        'jsonSchema': {
            'id': 'urn:jsonschema:edu:uci:ics:texera:dataflow:nlp:sentiment:NlpSentimentPredicate',
            'properties': {
                'attribute': {
                    'type': 'string'
                },
                'resultAttribute': {
                    'type': 'string'
                }
            },
            'required': [
                'attribute',
                'resultAttribute'
            ],
            'type': 'object'
        }
    },
    {
        'operatorType': 'ViewResults',
        'additionalMetadata': {
            'advancedOptions': [],
            'operatorDescription': 'View the results of the workflow',
            'operatorGroupName': 'View Results',
            'numInputPorts': 1,
            'numOutputPorts': 0,
            'userFriendlyName': 'View Results'
        },
        'jsonSchema': {
            'id': 'urn:jsonschema:edu:uci:ics:texera:dataflow:sink:tuple:TupleSinkPredicate',
            'properties': {
                'limit': {
                    'default': 10,
                    'type': 'integer'
                },
                'offset': {
                    'default': 0,
                    'type': 'integer'
                }
            },
            'type': 'object'
        }
    }
];

export const MOCK_OPERATOR_GROUPS: GroupInfo[] = [
    { groupName: 'Source', groupOrder: 1 },
    { groupName: 'Analysis', groupOrder: 2 },
    { groupName: 'View Results', groupOrder: 3 },
];

export const MOCK_OPERATOR_METADATA: OperatorMetadata = {
    operators: MOCK_OPERATOR_SCHEMA_LIST,
    groups: MOCK_OPERATOR_GROUPS
};
