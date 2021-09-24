package edu.uci.ics.texera.web.model.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CacheStatus {
    CACHE_INVALID("cache invalid"),
    CACHE_VALID("cache valid"),
    CACHE_NOT_ENABLED("cache not enabled");

    CacheStatus(String status) {
        this.status = status;
    }

    private final String status;

    @JsonValue
    public String getStatus() {
        return this.status;
    }

}
