package edu.uci.ics.texera.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.schema.Schema;

public class TableMetadata {
	private String tableName;
	private Schema schema;

	@JsonCreator
	public TableMetadata(
		@JsonProperty(value = JsonConstants.TABLE_NAME, required = true)
		String tableName,
		@JsonProperty(value = JsonConstants.SCHEMA, required = true)
		Schema schema) {
		this.tableName = tableName;
		this.schema = schema;
	}

	@JsonProperty(value = JsonConstants.TABLE_NAME)
	public String getTableName() {
		return tableName;
	}

	@JsonProperty(value = JsonConstants.SCHEMA)
	public Schema getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(tableName).append("=[").append(schema.getAttributes()).append("]");
		return sb.toString();
	}
}
