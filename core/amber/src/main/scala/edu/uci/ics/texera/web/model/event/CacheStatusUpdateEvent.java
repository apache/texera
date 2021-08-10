package edu.uci.ics.texera.web.model.event;

import com.fasterxml.jackson.annotation.JsonValue;
import scala.collection.Map;

public class CacheStatusUpdateEvent implements TexeraWebSocketEvent {

  public enum CacheStatus {

    CACHE_INVALID("CACHE_INVALID"),
    CACHE_VALID("CACHE_VALID"),
    CACHE_NOT_ENABLED("CACHE_NOT_ENABLED");

    CacheStatus(String status) {
      this.status = status;
    }

    private final String status;

    @JsonValue
    public String getStatus() {
      return this.status;
    }

  }

  private final Map<String, CacheStatus> cacheStatusMap;

  public CacheStatusUpdateEvent(Map<String, CacheStatus> cacheStatusMap) {
    this.cacheStatusMap = cacheStatusMap;
  }

}
