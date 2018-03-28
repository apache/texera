
/**
 * OPERATOR_METADATA const variable is a mock operator metadata consisting of
 *  a few operators, which can be useful when testing.
 *
 */
export const OPERATOR_METADATA: OperatorSchema[] = [
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
            'operatorGroupName': 'standalone',
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
