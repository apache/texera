package edu.uci.ics.texera.dataflow.source.asterix;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class AsterixSourcePredicate extends PredicateBase {
    
    private final String resultAttribute;
    private final String host;
    private final Integer port;
    private final String dataverse;
    private final String dataset;
    private final String field;
    private final String keyword;
    private final String startDate;
    private final String endDate;
    private final Integer limit;
    
    @JsonCreator
    public AsterixSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttribute,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_HOST, required = true)
            String host,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_PORT, required = true)
            Integer port,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_DATAVERSE, required = true)
            String dataverse,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_DATASET, required = true)
            String dataset,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_QUERY_FIELD, required = false)
            String field,
            @JsonProperty(value = PropertyNameConstants.KEYWORD_QUERY, required = false)
            String keyword,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_START_DATE, required = false)
            String startDate,
            @JsonProperty(value = PropertyNameConstants.ASTERIX_END_DATE, required = false)
            String endDate,
            @JsonProperty(value = PropertyNameConstants.LIMIT, required = false)
            Integer limit
            ) {
        Preconditions.checkArgument(resultAttribute != null && ! resultAttribute.isEmpty());
        this.resultAttribute = resultAttribute.trim();
        
        this.host = host.trim();
        this.port = port;
        this.dataverse = dataverse.trim();
        this.dataset = dataset.trim();
        if (field == null || field.trim().isEmpty()) {
            this.field = null;
        } else {
            this.field = field.trim();
        }
        if (keyword == null || keyword.trim().isEmpty()) {
            this.keyword = null;
        } else {
            this.keyword = keyword.trim();
        }
        if(startDate == null || startDate.trim().isEmpty() || !isValidDate(startDate)){
        	this.startDate = null;
        } else{
        	this.startDate = startDate.trim();
        }
        if(endDate == null || endDate.trim().isEmpty() || !isValidDate(endDate)){
        	this.endDate = null;
        } else{
        	this.endDate = endDate.trim();
        }
        if (limit == null || limit < 0) {
            this.limit = null;
        } else {
            this.limit = limit;
        }
    }
    
    private static boolean isValidDate(String inDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
          dateFormat.parse(inDate.trim());
        } catch (ParseException pe) {
          return false;
        }
        return true;
      }
    
    @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttribute() {
        return this.resultAttribute;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_HOST)
    public String getHost() {
        return host;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_PORT)
    public Integer getPort() {
        return port;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_DATAVERSE)
    public String getDataverse() {
        return dataverse;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_DATASET)
    public String getDataset() {
        return dataset;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_QUERY_FIELD)
    public String getField() {
        return field;
    }
    
    @JsonProperty(value = PropertyNameConstants.KEYWORD_QUERY)
    public String getKeyword() {
        return keyword;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_START_DATE)
    public String getStartDate() {
        return startDate;
    }
    
    @JsonProperty(value = PropertyNameConstants.ASTERIX_END_DATE)
    public String getEndDate() {
        return endDate;
    }
    
    @JsonProperty(value = PropertyNameConstants.LIMIT)
    public Integer getLimit() {
        return limit;
    }
    
    @Override
    public AsterixSource newOperator() {
        return new AsterixSource(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Asterix")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Connect to an AsterixDB instance")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }
    
}
