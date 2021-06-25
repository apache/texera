package edu.uci.ics.texera.api.span;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;

public class Span {
    // The name of the field (in the tuple) where this span is present
    private String attributeName;
    // The start position of the span, which is the offset of the gap before the
    // first character of the span.
    private int start;
    // The end position of the span, which is the offset of the gap after the
    // last character of the span.
    private int end;
    // The key we are searching for eg: regex
    private String key;
    // The value matching the key
    private String value;
    // The token position of the span, starting from 0.
    private int tokenOffset;

    /*
     * Example: Value = "The quick brown fox jumps over the lazy dog" Now the
     * Span for brown should be start = 10 : index Of character 'b' end = 15 :
     * index of character 'n'+ 1 OR start+length Both of then result in same
     * values. tokenOffset = 2 position of word 'brown'
     */
    public static int INVALID_TOKEN_OFFSET = -1;

    @JsonCreator
    public Span(
            @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME, required = true)
            String attributeName, 
            @JsonProperty(value = JsonConstants.SPAN_START, required = true)
            int start, 
            @JsonProperty(value = JsonConstants.SPAN_END, required = true)
            int end, 
            @JsonProperty(value = JsonConstants.SPAN_KEY, required = true)
            String key, 
            @JsonProperty(value = JsonConstants.SPAN_VALUE, required = true)
            String value,
            @JsonProperty(value = JsonConstants.SPAN_TOKEN_OFFSET, required = true)
            int tokenOffset) {
        this.attributeName = attributeName;
        this.start = start;
        this.end = end;
        this.key = key;
        this.value = value;
        this.tokenOffset = tokenOffset;
    }

    public Span(String attributeName, int start, int end, String key, String value) {
        this(attributeName, start, end, key, value, INVALID_TOKEN_OFFSET);
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty(value = JsonConstants.SPAN_START)
    public int getStart() {
        return start;
    }

    @JsonProperty(value = JsonConstants.SPAN_END)
    public int getEnd() {
        return end;
    }
    
    @JsonProperty(value = JsonConstants.SPAN_KEY)
    public String getKey() {
        return key;
    }

    @JsonProperty(value = JsonConstants.SPAN_VALUE)
    public String getValue() {
        return value;
    }

    @JsonProperty(value = JsonConstants.SPAN_TOKEN_OFFSET)
    public int getTokenOffset() {
        return tokenOffset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + end;
        result = prime * result + ((attributeName == null) ? 0 : attributeName.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + start;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        result = prime * result + tokenOffset;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Span other = (Span) obj;

        if (attributeName == null) {
            if (other.attributeName != null)
                return false;
        } else if (!attributeName.equals(other.attributeName))
            return false;

        if (start != other.start)
            return false;

        if (end != other.end)
            return false;

        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;

        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;

        if (tokenOffset != other.tokenOffset)
            return false;

        return true;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("field: " + this.getAttributeName() + "\n");
        sb.append("start: " + this.getStart() + "\n");
        sb.append("end:   " + this.getEnd() + "\n");
        sb.append("key:   " + this.getKey() + "\n");
        sb.append("value: " + this.getValue() + "\n");
        sb.append("token offset: " + this.getTokenOffset() + "\n");

        return sb.toString();
    }
}
