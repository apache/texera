package edu.uci.ics.textdb.api.common;

public enum FieldType {
    // A field that is indexed but not tokenized: the entire String
    // value is indexed as a single token
    STRING, INTEGER, DOUBLE, DATE,
    // A field that is indexed and tokenized,without term vectors
    TEXT,
    _ID_TYPE,
    // A field that is the list of values
    LIST;
}
