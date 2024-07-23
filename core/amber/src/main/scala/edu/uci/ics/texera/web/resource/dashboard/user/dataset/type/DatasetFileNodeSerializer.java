package edu.uci.ics.texera.web.resource.dashboard.user.dataset.type;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;

import java.io.IOException;

// this class is used to serialize the FileNode as JSON. So that FileNodes can be inspected by the frontend through JSON.
public class DatasetFileNodeSerializer extends StdSerializer<DatasetFileNode> {

  public DatasetFileNodeSerializer() {
    this(null);
  }

  public DatasetFileNodeSerializer(Class<DatasetFileNode> t) {
    super(t);
  }

  @Override
  public void serialize(DatasetFileNode value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("name", value.getName());
    gen.writeStringField("type", value.getNodeType());
    gen.writeStringField("parentDir", value.getParent().getFilePath());
    gen.writeStringField("ownerEmail", value.getOwnerEmail());
    if (value.getNodeType().equals("directory")) {
      gen.writeFieldName("children");
      gen.writeStartArray();
      List<DatasetFileNode> children = value.getChildren();
      for (DatasetFileNode child : JavaConverters.seqAsJavaList(children)) {
        serialize(child, gen, provider); // Recursively serialize children
      }
      gen.writeEndArray();
    }
    gen.writeEndObject();
  }
}
