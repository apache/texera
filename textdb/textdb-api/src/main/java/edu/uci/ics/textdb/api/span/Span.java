package edu.uci.ics.textdb.api.span;

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

    public Span(String attributeName, int start, int end, String key, String value) {
        this.attributeName = attributeName;
        this.start = start;
        this.end = end;
        this.key = key;
        this.value = value;
        this.tokenOffset = INVALID_TOKEN_OFFSET;
    }

    public Span(String attributeName, int start, int end, String key, String value, int tokenOffset) {
        this(attributeName, start, end, key, value);
        this.tokenOffset = tokenOffset;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

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
}
