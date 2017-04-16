package edu.uci.ics.textdb.storage;

import edu.uci.ics.textdb.api.schema.Schema;

public class TableMetadata {
	private String tableName;
	private Schema schema;

	public TableMetadata(String tableName, Schema schema) {
		this.tableName = tableName;
		this.schema = schema;
	}

	public String getTableName() {
		return tableName;
	}

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
