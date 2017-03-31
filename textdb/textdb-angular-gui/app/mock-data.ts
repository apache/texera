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
            limit : "10",
            offset : "5"
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
      regex : "zika\s*(virus|fever)",
      limit : "100",
      attributes : "first name, last name",
      offset : "5"
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
      limit : "100",
      offset : "10"
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
      limit : "100",
      offset : "5",
    }
  }
}

let nlpMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'NLPMatcher',
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
      nlp_type : "noun",
      attributes : "first name, last name",
      limit : "100",
      offset : "5"
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
      attributes : "firstname, lastname",
      limit : "100",
      offset : "5",
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
      limit: "1",
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
      inner_attribute : "name1",
      outer_attribute : "name2",
      limit : "100",
      offset : "5"
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
      limit : "100",
      offset : "5",
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
      attributes : "firstname, lastname",
      limit : "100000",
      offset : "0",
    }
  }
}

export const DEFAULT_DATA: Data[] = [
    {id: 1, jsonData: {}}
];

export const DEFAULT_MATCHERS: Data[] = [
    {id: 0, jsonData: regexMatcher},
    {id: 1, jsonData: keywordMatcher},
    {id: 2, jsonData: dictionaryMatcher},
    {id: 3, jsonData: FuzzyMatcher},
    {id: 4, jsonData: nlpMatcher},
    {id: 5, jsonData: Projection},
    {id: 6, jsonData: keywordSource},
    {id: 7, jsonData: Join},
    {id: 8, jsonData: fileOutput},
    {id: 9, jsonData: Result}
];
