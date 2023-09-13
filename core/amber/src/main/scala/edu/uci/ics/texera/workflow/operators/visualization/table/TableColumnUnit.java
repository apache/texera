package edu.uci.ics.texera.workflow.operators.visualization.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameLambda;

import java.util.Objects;

public class TableColumnUnit {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Name")
  @AutofillAttributeNameLambda
  public String columnName;

  public TableColumnUnit(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableColumnUnit)) return false;
    TableColumnUnit that = (TableColumnUnit) o;
    return Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName);
  }
}
