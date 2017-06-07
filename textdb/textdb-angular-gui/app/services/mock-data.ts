import { Data } from './data';

let keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'Keyword Search',
        inputs: {
            input_1: {
                label: 'Input (:i)',
            }
        },
        outputs: {
            output_1: {
                label: 'Output (:i)',
            }
        },
        attributes: {
            "operatorType": "KeywordMatcher",
            "attributes": [],
            "query": "keyword",
            "luceneAnalyzer": "standard",
            "matchingType": "phrase",
            "spanListName": " "
        }
    }
};

let regexMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Match',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : 'Output (:i)',
      }
    },
    attributes : {
        "operatorType": "RegexMatcher",
        "attributes": [],
        "regex": "regex",
        "regexIgnoreCase": false,
        "spanListName": " "
    }
  }
};

let dictionaryMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'Dictionary Search',
    inputs : {
      input_1 : {
        label : "Input(:i)",
      }
    },
    outputs :{
      output_1 : {
        label : "Output(:i)",
      }
    },
    attributes :  {
        "operatorType": "DictionaryMatcher",
        "attributes": [],
        "dictionaryEntries": [],
        "luceneAnalyzer": "standard",
        "matchingType": "phrase",
        "spanListName": " "
    }
  }
}

let fuzzyMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : "Fuzzy Token Match",
    inputs : {
      input_1 : {
        label : "Input(:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output(:i)",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenMatcher",
        "attributes": [],
        "query": "token1 token2 token3",
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "spanListName": " ",
    }
  }
}

let nlpEntity = {
  top : 20,
  left : 20,
  properties : {
    title : 'Entity recognition',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "NlpEntity",
        "attributes": [],
        "nlpEntityType": "location",
        "spanListName": " "
    }
  }
}

let nlpSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sentiment Analysis',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "NlpSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    }
  }
}

let emojiSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'Emoji Sentiment Analysis',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "EmojiSentiment",
        "attribute": "",
        "resultAttribute": "resultAttr"
    }
  }
}

let regexSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Regex Split',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "RegexSplit",
        "splitAttribute": "attr1",
        "splitRegex": "regex",
        "splitType": "standalone"
    }
  }
}

let nlpSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'Nlp Sentence Split',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "NlpSplit",
        "attribute": "",
        "resultAttribute": "resultAttribute",
        "splitOption": "oneToOne"
    }
  }
}

let sampler = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sampling',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "Sampler",
        "sampleSize": 10,
        "sampleType": "firstk"
    }
  }
}

let projection = {
  top : 20,
  left : 20,
  properties : {
    title : 'Projection',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "Projection",
        "attributes": []
    }
  }
}

let fileSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: File',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "FileSource",
        "filePath": "",
        "resultAttribute": "",
    }
  }
}

let scanSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Scan',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "ScanSource",
        "tableName": ""
    }
  }
}

let keywordSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Keyword',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
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
    }
  }
}


let dictionarySource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Dictionary',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
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
    }
  }
}

let regexSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Regex',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
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
    } 
  }
}

let fuzzyTokenSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: FuzzyToken',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
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
    }
  }
}

let wordCountSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'Source: Word Count',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "WordCountIndexSource",
        "tableName": "",
        "attribute": "",
    }
  }
}

let wordCount = {
  top : 20,
  left : 20,
  properties : {
    title : 'Word Count',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "WordCount",
        "attribute": "",
	      "luceneAnalyzer": "standard",
    }
  }
}

let comparison = {
  top : 20,
  left : 20,
  properties : {
    title : 'Comparison',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "Comparison",
        "attribute": "",
        "comparisonType": "=",
	      "compareTo": "",
    }
  }
}

let characterDistanceJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Character Distance',
    inputs : {
      input_1 : {
        label : 'Input (:i)',
      },
      input_2 : {
        label : "Input 2",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "JoinDistance",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "spanDistance": 100
    }
  }
}

let similarityJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join: Similarity',
    inputs : {
      input_1 : {
        label : 'Input (:i)',
      },
      input_2 : {
        label : "Input 2",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "SimilarityJoin",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "similarityThreshold": 0.8
    }
  }
}

let result = {
  top : 20,
  left : 20,
  properties : {
    title : 'View Results',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "ViewResults",
        "limit": 10,
        "offset": 0,
    }
  }
}

let excelSink = {
  top : 20,
  left : 20,
  properties : {
    title : 'Write Excel',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "ExcelSink",
        "limit": 10,
        "offset": 0,
    }
  }
}

let mysqlSink = {
  top : 20,
  left : 20,
  properties : {
    title : 'Write Mysql',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
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
    }
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
    {id: 20, jsonData: excelSink},
    {id: 21, jsonData: comparison},
    {id: 22, jsonData: nlpSplit},
    {id: 23, jsonData: emojiSentiment},
    {id: 24, jsonData: fileSource},
    {id: 25, jsonData: mysqlSink},
];
