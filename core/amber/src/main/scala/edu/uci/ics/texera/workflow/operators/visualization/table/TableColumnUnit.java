package edu.uci.ics.texera.workflow.operators.visualization.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameLambda;

import java.util.Objects;

public class TableColumnUnit {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Name")
  @AutofillAttributeNameLambda
  public String attributeName;

  public TableColumnUnit(String attributeName) {
    this.attributeName = attributeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableColumnUnit that)) return false;
    return attributeName.equals(that.attributeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributeName);
  }
}
