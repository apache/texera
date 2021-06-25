package edu.uci.ics.texera.dataflow.source.mysql;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class MysqlSourcePredicate extends PredicateBase{
    private final String host;
    private final Integer port;
    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final Integer limit;
    private final Integer offset;
    private final String column;
    private final String keywords;


    @JsonCreator
    public MysqlSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.MYSQL_HOST, required = true)
            String host,
            @JsonProperty(value = PropertyNameConstants.MYSQL_PORT, required = true)
            Integer port,
            @JsonProperty(value = PropertyNameConstants.MYSQL_DATABASE, required = true)
            String database,
            @JsonProperty(value = PropertyNameConstants.MYSQL_TABLE, required = true)
            String table,	            
            @JsonProperty(value = PropertyNameConstants.MYSQL_USERNAME, required = true)
            String username,
            @JsonProperty(value = PropertyNameConstants.MYSQL_PASSWORD, required = true)
            String password,
            @JsonProperty(value = PropertyNameConstants.MYSQL_LIMIT, required = false)
            Integer limit,
            @JsonProperty(value = PropertyNameConstants.MYSQL_OFFSET, required = false)
            Integer offset,
            @JsonProperty(value = PropertyNameConstants.MYSQL_COLUMN, required = false)
            String column,
            @JsonProperty(value = PropertyNameConstants.MYSQL_KEYWORDS, required = false)
            String keywords
            ) {
        this.host = host.trim();
        this.port = port;
        this.database = database.trim();
        this.table = table.trim();
        this.username = username.trim();	
        this.password = password;	// Space should be legitimate password
        this.limit = limit == null ? Integer.MAX_VALUE : limit;
        this.offset = offset == null ? 0 : offset;
        this.column = column == null ? "" : column;
        this.keywords = keywords == null ? "" : keywords;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_HOST)
    public String getHost() {
        return host;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_PORT)
    public Integer getPort() {
        return port;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_DATABASE)
    public String getDatabase() {
        return database;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_COLUMN)
    public String getColumn() {
    	return column;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_TABLE)
    public String getTable() {
        return table;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_USERNAME)
    public String getUsername() {
        return username;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_PASSWORD)
    public String getPassword() {
        return password;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_LIMIT)
    public Integer getLimit() {
        return limit;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_OFFSET)
    public Integer getOffset() {
        return offset;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_KEYWORDS)
    public String getKeywords() {
        return keywords;
    }
    
    @Override
    public MysqlSource newOperator() {
        return new MysqlSource(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Mysql")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Connect to an Mysql instance")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }
    
}
