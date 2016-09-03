package edu.uci.ics.textdb.dataflow.jsonplangen;

public class SampleJsonQuery {
    
    public static String sampleQuery1 = "{\n" + 
            "    \"operators\" : [\n" + 
            "        {\n" + 
            "            \"id\" : \"data_source\",\n" + 
            "            \"operatorType\" : \"IndexBasedSource\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"directory\" : \"./index\",\n" + 
            "                \"luceneAnalyzer\" : \"standard\",\n" + 
            "                \"schema\" : {\n" + 
            "                    \"id\" : \"int\",\n" + 
            "                    \"content\" : \"text\"\n" + 
            "                }\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"keyword_zika\",\n" + 
            "            \"operatorType\" : \"KeywordMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"keyword\" : \"zika\",\n" + 
            "                \"attributes\" : \"content\",\n" + 
            "                \"matchingType\" : \"conjunction\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"regex_person\",\n" + 
            "            \"operatorType\" : \"RegexMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"regex\" : \"\\\\b(woman)|(man)|(patient)\\\\b\",\n" + 
            "                \"attributes\" : \"content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"regex_date\",\n" + 
            "            \"operatorType\" : \"RegexMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"regex\" : \"date about regex\",\n" + 
            "                \"attributes\" : \"content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"nlp_location\",\n" + 
            "            \"operatorType\" : \"NlpExtractor\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"nlpType\" : \"Location\",\n" + 
            "                \"attributes\" : \"content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"join_person_location\",\n" + 
            "            \"operatorType\" : \"Join\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"predicate\" : {\n" + 
            "                    \"predicateType\" : \"characterOffset\",\n" + 
            "                    \"predicateCondition\" : \"100\"\n" + 
            "                },\n" + 
            "                \"attributes\" : \"content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"join_person_location_date\",\n" + 
            "            \"operatorType\" : \"Join\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"predicate\" : {\n" + 
            "                    \"predicateType\" : \"characterOffset\",\n" + 
            "                    \"predicateCondition\" : \"100\"\n" + 
            "                },\n" + 
            "                \"attributes\" : \"content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"sink_result\",\n" + 
            "            \"operatorType\" : \"FileSink\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"path\" : \"./result.txt\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"projection_content_1\",\n" + 
            "            \"operatorType\" : \"Projection\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"attributes\" : \"id, content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"projection_content_2\",\n" + 
            "            \"operatorType\" : \"Projection\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"attributes\" : \"id, content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"projection_content_3\",\n" + 
            "            \"operatorType\" : \"Projection\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"attributes\" : \"id, content\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"projection_span\",\n" + 
            "            \"operatorType\" : \"Projection\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"attributes\" : \"id, content\"\n" + 
            "            }\n" + 
            "        }\n" + 
            "    ],\n" + 
            "\n" + 
            "    \"links\" : [\n" + 
            "        {\n" + 
            "            \"from\" : \"data_source\",\n" + 
            "            \"to\"   : \"keyword_zika\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"keyword_zika\",\n" + 
            "            \"to\"   : \"projection_content_1\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"keyword_zika\",\n" + 
            "            \"to\"   : \"projection_content_2\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"keyword_zika\",\n" + 
            "            \"to\"   : \"projection_content_3\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"projection_content_1\",\n" + 
            "            \"to\"   : \"regex_person\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"projection_content_2\",\n" + 
            "            \"to\"   : \"regex_date\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"projection_content_3\",\n" + 
            "            \"to\"   : \"nlp_location\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"regex_person\",\n" + 
            "            \"to\"   : \"join_person_location\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"nlp_location\",\n" + 
            "            \"to\"   : \"join_person_location\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"regex_date\",\n" + 
            "            \"to\"   : \"join_person_location_date\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"join_person_location\",\n" + 
            "            \"to\"   : \"join_person_location_date\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"join_person_location_date\",\n" + 
            "            \"to\"   : \"projection_span\"\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"from\" : \"projection_span\",\n" + 
            "            \"to\"   : \"sink_result\"\n" + 
            "        }\n" + 
            "    ]\n" + 
            "}\n" + 
            "";
    
    
    
    public static String sampleJsonQueryKeywordMatcher = "{\n" + 
            "    \"operators\" : [\n" + 
            "        {\n" + 
            "            \"id\" : \"keyword_zika\",\n" + 
            "            \"operatorType\" : \"KeywordMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"keyword\" : \"zika\",\n" + 
            "                \"attributeNames\" : \"content\",\n" + 
            "                \"attributeTypes\" : \"text\",\n" + 
            "                \"matchingType\" : \"conjunction_indexbased\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"keyword_irvine\",\n" + 
            "            \"operatorType\" : \"KeywordMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"keyword\" : \"Irvine\",\n" + 
            "                \"attributeNames\" : \"city, location, content\",\n" + 
            "                \"attributeTypes\" : \"string, string, text\",\n" + 
            "                \"matchingType\" : \"substring_scanbased\",\n" + 
            "                \"limit\" : \"10\",\n" + 
            "                \"offset\" : \"2\"\n" + 
            "            }\n" + 
            "        }\n" + 
            "    ],\n" + 
            "\n" + 
            "    \"links\" : [\n" + 
            "    ]\n" + 
            "}\n" + 
            "";
    
    public static String sampleInvalidJsonQueryKeywordMatcher = "{\n" + 
            "    \"operators\" : [\n" + 
            "        {\n" + 
            "            \"id\" : \"keyword_zika\",\n" + 
            "            \"operatorType\" : \"KeywordMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"keyword\" : \"\",\n" + 
            "                \"attributeNames\" : \"content\",\n" + 
            "                \"attributeTypes\" : \"text\",\n" + 
            "                \"matchingType\" : \"conjunction_indexbased\"\n" + 
            "            }\n" + 
            "        }\n" + 
            "    ],\n" + 
            "\n" + 
            "    \"links\" : [\n" + 
            "    ]\n" + 
            "}\n" + 
            "";
    
    public static String sampleJsonQueryRegexMatcher = "{\n" + 
            "    \"operators\" : [\n" + 
            "        {\n" + 
            "            \"id\" : \"regex_person\",\n" + 
            "            \"operatorType\" : \"RegexMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"regex\" : \"\\\\b(woman)|(man)|(patient)\\\\b\",\n" + 
            "                \"attributeNames\" : \"content\",\n" + 
            "                \"attributeTypes\" : \"text\"\n" + 
            "            }\n" + 
            "        },\n" + 
            "        {\n" + 
            "            \"id\" : \"regex_date\",\n" + 
            "            \"operatorType\" : \"RegexMatcher\",\n" + 
            "            \"properties\" : {\n" + 
            "                \"regex\" : \"date about regex\",\n" + 
            "                \"attributeNames\" : \"content\",\n" + 
            "                \"attributeTypes\" : \"text\"\n" + 
            "            }\n" + 
            "        }\n" + 
            "    ],\n" + 
            "\n" + 
            "    \"links\" : [\n" + 
            "    ]\n" + 
            "}";
    
}
