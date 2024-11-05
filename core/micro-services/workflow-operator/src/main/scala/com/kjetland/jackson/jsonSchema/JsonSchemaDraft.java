package com.kjetland.jackson.jsonSchema;

public enum JsonSchemaDraft {
    DRAFT_04("http://json-schema.org/draft-04/schema#"),
    DRAFT_06("http://json-schema.org/draft-06/schema#"),
    DRAFT_07("http://json-schema.org/draft-07/schema#"),
    DRAFT_2019_09("http://json-schema.org/draft/2019-09/schema#");

    final String url;

    JsonSchemaDraft(String url) {
        this.url = url;
    }
}