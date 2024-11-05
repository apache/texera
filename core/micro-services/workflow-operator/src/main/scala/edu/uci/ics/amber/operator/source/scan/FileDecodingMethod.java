package edu.uci.ics.amber.operator.source.scan;

import com.fasterxml.jackson.annotation.JsonValue;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public enum FileDecodingMethod {
    UTF_8("UTF_8", StandardCharsets.UTF_8),
    UTF_16("UTF_16", StandardCharsets.UTF_16),
    ASCII("US_ASCII", StandardCharsets.US_ASCII);

    private final String name;
    private final Charset charset;

    FileDecodingMethod(String name, Charset charset) {
        this.name = name;
        this.charset = charset;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public Charset getCharset() {return this.charset;}
}
