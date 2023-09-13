package edu.uci.ics.texera.workflow.operators.visualization.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameLambda;

import java.util.Objects;

public class TableColumnNameUnit {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Column Name")
  @AutofillAttributeNameLambda
  public String columnName;

  public TableColumnNameUnit(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableColumnNameUnit)) return false;
    TableColumnNameUnit that = (TableColumnNameUnit) o;
    return Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName);
  }
}
