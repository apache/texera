package edu.uci.ics.texera.workflow.operators.visualization.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameLambda;

import java.util.Objects;

public class TableHeaderUnit {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Name")
  @AutofillAttributeNameLambda
  public String attributeName;

  public TableHeaderUnit(String attributeName) {
    this.attributeName = attributeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableHeaderUnit)) return false;
    TableHeaderUnit that = (TableHeaderUnit) o;
    return Objects.equals(attributeName, that.attributeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributeName);
  }
}
