import { Data } from './data';

let keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'Keyword Search',
        inputs: {
            input_1: {
                label: '',
            }
        },
        outputs: {
            output_1: {
                label: '',
            }
        },
        attributes: {
            "operatorType": "KeywordMatcher",
            "attributes": [],
            "query": "keyword",
            "luceneAnalyzer": "standard",
            "matchingType": "phrase",
            "spanListName": " "
        },
        image : 'thirdparty/images/keywordSearch.png',
        color : '#80bfff',
        description: "Search the documents using a keyword",
    }
};

let regexMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Match',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : '',
      }
    },
    attributes : {
        "operatorType": "RegexMatcher",
        "attributes": [],
        "regex": "regex",
        "regexIgnoreCase": false,
        "spanListName": " "
    },
    image : 'thirdparty/images/regexMatch.png',
    color : '#66b3ff',
    description: "Search the documents using a regular expression"
  }
};

let dictionaryMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Dictionary Search',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs :{
      output_1 : {
        label : "",
      }
    },
    attributes :  {
        "operatorType": "DictionaryMatcher",
        "attributes": [],
        "dictionaryEntries": [],
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : 'thirdparty/images/dictionarySearch.png',
    color : '#66b3ff',
    description: "Search the documents using a dictionary (multiple keywords)",
  }
}

let fuzzyMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : "Fuzzy Token Match",
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenMatcher",
        "attributes": [],
        "query": "token1 token2 token3",
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "spanListName": " ",
    },
    image : 'thirdparty/images/fuzzy.png',
    color : '#66b3ff',
    description: "Search the documents according to the similarity of given tokens",
  }
}

let nlpEntity = {
  top : 20,
  left : 20,
  properties : {
    title : 'Entity recognition',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpEntity",
        "attributes": [],
        "nlpEntityType": "location",
        "spanListName": " "
    },
    image : 'thirdparty/images/entityRecognition.png',
    color : '#85e085',
    description: "Recognize entities in the text (person, location, date, ..)",
  }
}

let nlpSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    },
    image : 'thirdparty/images/sentimentAnalysis.png',
    color : '#85e085',
    description: "Sentiment analysis based on Stanford NLP package",
  }
}

let emojiSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Emoji Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "EmojiSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    },
    image : 'thirdparty/images/emojiAnalysis.png',
    color : '#85e085',
    description: "Sentiment analysis with the emojis in consideration",
  }
}

let nltkSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Nltk Sentiment Analysis',
    inputs : {
      input_1 : {
        label : '',
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NltkSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr",
        "batchSize": "1000",
        "inputAttributeModel": "NltkSentiment.pickle"
    },
    image : "thirdparty/images/nltk.jpg",
    color : '#85e085',
    description: "Sentiment analysis based on Python's NLTK package",
  }
}

let regexSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Split',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "RegexSplit",
        "attribute": "",
        "splitRegex": "regex",
        "resultAttribute": "splitText",
        "splitType": "standalone",
        "splitOption": "oneToMany"
    },
    image : 'thirdparty/images/regex.png',
    color : '#e6e600',
    description: "Split the text into multiple segments based on a regular expression",
  }
}

let nlpSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Nlp Sentence Split',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "NlpSplit",
        "attribute": "",
        "resultAttribute": "splitText",
        "splitOption": "oneToMany",
    },
    image : 'thirdparty/images/sentencesplit.png',
    color : '#e6e600',
    description: "Automatically split the text into multiple sentences using Natural Language Processing ",
  }
}

let sampler = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sampling',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Sampler",
        "sampleSize": 10,
        "sampleType": "firstk"
    },
    image : 'thirdparty/images/sampling.png',
    color : '#ffdb4d',
    description: "Sample a subset of data from all the documents",
  }
}

let projection = {
  top : 20,
  left : 20,
  properties : {
    title : 'Projection',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Projection",
        "attributes": []
    },
    image : 'thirdparty/images/projection.png',
    color : '#ffdb4d',
    description: "Select a subset of columns",
  }
}

let asterixSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Asterix',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "AsterixSource",
        "host": "texera.ics.uci.edu",
        "port": 19002,
        "dataverse": "twitter",
        "dataset": "ds_tweet",
        "queryField": "text",
        "query": "drug",
        "limit": 100000,
    },
    image : 'thirdparty/images/asterixSource.png',
    color : '#cc99ff',
    description: "Connect to an AsterixDB instance",
  }
}

let fileSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: File',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FileSource",
        "filePath": "",
        "resultAttribute": "",
    },
    image : 'thirdparty/images/fileSource.png',
    color : '#cc99ff',
    description: "Read the content of one file or multiple files.",
  }
}

let scanSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Scan',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "ScanSource",
        "tableName": ""
    },
    image : 'thirdparty/images/scan.png',
    color : '#cc99ff',
    description: "Read records from a table one by one",
  }
}

 let twitterfeedSource = {
     top : 20,
     left : 20,
     properties : {
         title : 'Source: TwitterFeed',
         inputs : {
             input_1 : {
                 label : "",
             }
         },
         outputs : {
             output_1 : {
                 label : "",
             }
         },
         attributes : {
             "operatorType": "TwitterFeed",
             "keywordList": [],
             "locationList": "",
             "tweetNum": 10,
             "customerKey": "",
             "customerSecret": "",
             "token": "",
             "tokenSecret": "",
             "languageList": ["en"]
         },
         image: 'thirdparty/images/twitterFeed.png',
         color: '#cc99ff',
         description: "Obtain real-time tweets using Twitter API",
     }
 }

let keywordSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Keyword',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "KeywordSource",
        "tableName": "",
        "attributes": [],
        "query": "keyword",
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : 'thirdparty/images/keywordSource.png',
    color : '#cc99ff',
    description: "Perform an index-based search on a table using a keyword",
  }
}


let dictionarySource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Dictionary',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "DictionarySource",
        "tableName": "",
        "attributes": [],
        "dictionaryEntries": [],
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    },
    image : 'thirdparty/images/dictionary.png',
    color : '#cc99ff',
    description: "Perform an index-based search on a table using a dictionary",

  }
}

let regexSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Regex',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "RegexSource",
        "tableName": "",
        "attributes": [],
        "regex": "regex",
        "regexIgnoreCase": false,
        "regexUseIndex": true,
        "spanListName": " "
    },
    image : 'thirdparty/images/regex.png',
    color : '#cc99ff',
    description: "Perform an index-based search on a table using a regular expression",
  }
}

let fuzzyTokenSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: FuzzyToken',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenSource",
        "tableName": "",
        "attributes": [],
        "query": "token1 token2 token3",
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "spanListName": " ",
    },
    image : 'thirdparty/images/fuzzySource.png',
    color : '#cc99ff',
    description: "Perform an index-based search on a table for records similar to given tokens",
  }
}

let wordCountSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Word Count',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "WordCountIndexSource",
        "tableName": "",
        "attribute": "",
    },
    image : 'thirdparty/images/countSource.svg',
    color : '#cc99ff',
    description: "Count the frequency of for each word using index",
  }
}

let wordCount = {
  top : 20,
  left : 20,
  properties : {
    title : 'Word Count',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "WordCount",
        "attribute": "",
	      "luceneAnalyzer": "standard",
    },
    image : 'thirdparty/images/counting.png',
    color : '#ffdb4d',
    description: "Count the frequency of each word in all the documents",
  }
}

let twitterConverter = {
  top : 20,
  left : 20,
  properties : {
    title : 'Convert Twitter',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "TwitterConverter"
    },
    image : 'thirdparty/images/twitterConverer.png',
    color : '#ffdb4d',
    description: "Convert the raw twitter data to readable records",
  }
}

let comparison = {
  top : 20,
  left : 20,
  properties : {
    title : 'Comparison',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "Comparison",
        "attribute": "",
        "comparisonType": "=",
	      "compareTo": "",
    },
    image : 'thirdparty/images/compare.png',
    color : '#ffdb4d',
    description: "Select data based on a condition (>, <, =, ..)",
  }
}

let characterDistanceJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Character Distance',
    inputs : {
      input_1 : {
        label : '',
      },
      input_2 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "JoinDistance",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "spanDistance": 100
    },
    image : 'thirdparty/images/characterjoin.png',
    color : '#ffa366',
    description: "Join two tables based on the character distance of two attributes",
  }
}

let similarityJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Similarity',
    inputs : {
      input_1 : {
        label : '',
      },
      input_2 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "SimilarityJoin",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "similarityThreshold": 0.8
    },
    image : 'thirdparty/images/similarityjoin.png',
    color : '#ffa366',
    description: "Join two tables based on the string similarity of two tuples",
  }
}

let result = {
  top : 20,
  left : 20,
  properties : {
    title : 'View Results',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "ViewResults",
        "limit": 10,
        "offset": 0,
    },
    image : 'thirdparty/images/view-result.png',
    color : '#d2a679',
    description: "View the results of the workflow",
  }
}

let mysqlSink = {
  top : 20,
  left : 20,
  properties : {
    title : 'Write Mysql',
    inputs : {
      input_1 : {
        label : "",
      }
    },
    outputs : {
      output_1 : {
        label : "",
      }
    },
    attributes : {
        "operatorType": "MysqlSink",
        "host": "localhost",
        "port": 3306,
        "database": "testDB",
        "table": "testTable",
        "username": "test",
        "password": "test"
    },
    image : 'thirdparty/images/sql.jpg',
    color : '#ff8080',
    description: "Write the results to a mysql database",
  }
}

export const DEFAULT_MATCHERS: Data[] = [
    {id: 0, jsonData: regexMatcher},
    {id: 1, jsonData: keywordMatcher},
    {id: 2, jsonData: dictionaryMatcher},
    {id: 3, jsonData: fuzzyMatcher},
    {id: 4, jsonData: nlpEntity},
    {id: 5, jsonData: nlpSentiment},
    {id: 6, jsonData: regexSplit},
    {id: 7, jsonData: sampler},
    {id: 8, jsonData: projection},
    {id: 9, jsonData: scanSource},
    {id: 10, jsonData: keywordSource},
    {id: 11, jsonData: dictionarySource},
    {id: 12, jsonData: regexSource},
    {id: 13, jsonData: fuzzyTokenSource},
    {id: 14, jsonData: characterDistanceJoin},
    {id: 15, jsonData: similarityJoin},
    {id: 16, jsonData: wordCountSource},
    {id: 17, jsonData: wordCount},
    {id: 19, jsonData: result},
    {id: 21, jsonData: comparison},
    {id: 22, jsonData: nlpSplit},
    {id: 23, jsonData: emojiSentiment},
    {id: 24, jsonData: fileSource},
    {id: 25, jsonData: mysqlSink},
    {id: 26, jsonData: nltkSentiment},
    {id: 27, jsonData: asterixSource},
    {id: 28, jsonData: twitterConverter},
    {id: 29, jsonData: twitterfeedSource}
];
