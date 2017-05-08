import { Data } from './data';

let keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'KeywordMatcher',
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
            "query": "keyword",
            "attributes": [],
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
    title : 'RegexMatcher',
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
        "regex": "regex",
        "attributes": [],
        "regexIgnoreCase": false
        "spanListName": " "
    }
  }
};

let dictionaryMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'DictionaryMatcher',
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
        "dictionaryEntries": ["entry1", "entry2"],
        "attributes": [],
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
    title : "FuzzyTokenMatcher",
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
        "query": "token1 token2 token3",
        "attributes": [],
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
    title : 'NlpEntity',
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
        "nlpEntityType": "location",
        "attributes": ["attr1", "attr2"],
        "spanListName": " "
    }
  }
}

let nlpSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'NlpSentiment',
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
        "attribute": "inputAttr",
        "resultAttribute": "resultAttr"
    }
  }
}

let regexSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexSplit',
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
        "splitRegex": "regex",
        "splitAttribute": "attr1",
        "splitType": "standalone"
    }
  }
}

let sampler = {
  top : 20,
  left : 20,
  properties : {
    title : 'Sampler',
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
        "attributes": ["attr1", "attr2"]
    }
  }
}

let scanSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'ScanSource',
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
        "tableName": "tableName"
    }
  }
}

let keywordSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'KeywordSource',
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
        "query": "keyword",
        "attributes": [],
        "matchingType": "conjunction",
        "tableName": "",
        "spanListName": ""
    }
  }
}


let dictionarySource = {
  top : 20,
  left : 20,
  properties : {
    title : 'DictionarySource',
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
        "dictionaryEntries": ["entry1", "entry2"],
        "attributes": [],
        "luceneAnalyzer": "standard",
        "matchingType": "conjunction",
        "tableName": ""
        "spanListName": " "
    }
  }
}

let regexSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexSource',
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
        "regex": "regex",
        "attributes": [],
        "regexIgnoreCase": false,
        "tableName": "tableName",
        "regexUseIndex": true,
        "spanListName": " "
    } 
  }
}

let fuzzyTokenSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'FuzzyTokenSource',
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
        "query": "token1 token2 token3",
        "attributes": [],
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "tableName": "tableName",
        "spanListName": " ",
    }
  }
}

let characterDistanceJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'CharacterDistanceJoin',
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
    title : 'Similarity Join',
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
        "operatorType": "ExcelSink",
        "limit": 10,
        "offset": 0,
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
    {id: 19, jsonData: result},
    {id: 20, jsonData: excelSink}

];
