import { Data } from './data';

let defaultData = {
    operators: {
        operator1: {
            top: 20,
            left: 20,
            properties: {
                title: 'Operator 1',
                inputs: {},
                outputs: {
                    output_1: {
                        label: 'Output 1',
                    }
                }
            }
        },
        operator2: {
            top: 80,
            left: 300,
            properties: {
                title: 'Operator 2',
                inputs: {
                    input_1: {
                        label: 'Input 1',
                    },
                    input_2: {
                        label: 'Input 2',
                    },
                },
                outputs: {}
            }
        },
    },
    links: {
        link_1: {
            fromOperator: 'operator1',
            fromConnector: 'output_1',
            toOperator: 'operator2',
            toConnector: 'input_2',
        },
    }
};

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
            operator_type: "KeywordMatcher",
            keyword : "zika",
            matching_type : "conjunction",
            attributes : "content",
            limit : "1000000",
            offset : "0"
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
      operator_type : "RegexMatcher",
      regex : "\\b(A|a|(an)|(An))[^,.]{0,40} ((woman)|(man))\\b",
      limit : "1000000",
      attributes : "content",
      offset : "0"
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
      operator_type : "DictionaryMatcher",
      dictionary : "SampleDict1.txt",
      matching_type : "conjunction",
      attributes : "firstname, lastname",
      limit : "1000000",
      offset : "0"
    }
  }
}

let FuzzyMatcher = {
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
      operator_type : "FuzzyTokenMatcher",
      query : "FuzzyWuzzy",
      threshold_ratio : "0.8",
      attributes : "firstname, lastname",
      limit : "1000000",
      offset : "0",
    }
  }
}

let nlpMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'NlpExtractor',
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
      operator_type : "NlpExtractor",
      nlp_type : "location",
      attributes : "content",
      limit : "1000000",
      offset : "0"
    }
  }
}

let Projection = {
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
      operator_type : "Projection",
      attributes : "_id, content",
      limit : "1000000",
      offset : "0",
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
      operator_type : "KeywordSource",
      keyword : "zika",
      matching_type : "conjunction",
      data_source: "promed",
      attributes : "content",
      limit : "1000000",
      offset : "0",
    }
  }
}


let DictionarySource = {
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
      operator_type : "DictionarySource",
      dictionary : "SampleDict1.txt",
      matching_type : "conjunction",
      data_source: "promed",
      attributes : "content",
      limit : "1000000",
      offset : "0",

    }
  }
}

let RegexSource = {
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
      operator_type : "RegexSource",
      data_source: "promed",
      regex : "\\b(A|a|(an)|(An))[^,.]{0,40} ((woman)|(man))\\b",
      attributes : "content",
      limit : "1000000",
      offset : "0",
    }
  }
}

let FuzzyTokenSource = {
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
      operator_type : "FuzzyTokenSource",
      data_source: "promed",
      query : "FuzzyWuzzy",
	    threshold_ratio : "0.8",
      attributes : "content",
      limit : "1000000",
      offset : "0",
    }
  }
}



let Join = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join',
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
      operator_type : "Join",
      predicate_type : "CharacterDistance",
      threshold : "100",
      inner_attribute : "content",
      outer_attribute : "content",
      limit : "1000000",
      offset : "0"
    }
  }
}

let fileOutput = {
  top : 20,
  left : 20,
  properties : {
    title : 'FileSink',
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
      operator_type : "FileSink",
      file_path : "output.txt",
      attributes : "firstname, lastname",
      limit : "1000000",
      offset : "0",
    }
  }
}

let Result = {
  top : 20,
  left : 20,
  properties : {
    title : 'TupleStreamSink',
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
      operator_type : "TupleStreamSink",
      attributes : "content",
      limit : "1000000",
      offset : "0"
    }
  }
}

export const DEFAULT_DATA: Data[] = [
    {id: 1, jsonData: {}}
];
// DictionarySource, RegexSource, FuzzyTokenSource

export const DEFAULT_MATCHERS: Data[] = [
    {id: 0, jsonData: regexMatcher},
    {id: 1, jsonData: keywordMatcher},
    {id: 2, jsonData: dictionaryMatcher},
    {id: 3, jsonData: FuzzyMatcher},
    {id: 4, jsonData: nlpMatcher},
    {id: 5, jsonData: Projection},
    {id: 6, jsonData: keywordSource},
    {id: 7, jsonData: DictionarySource},
    {id: 8, jsonData: RegexSource},
    {id: 9, jsonData: FuzzyTokenSource},
    {id: 10, jsonData: Join},
    {id: 11, jsonData: fileOutput},
    {id: 12, jsonData: Result}
];
