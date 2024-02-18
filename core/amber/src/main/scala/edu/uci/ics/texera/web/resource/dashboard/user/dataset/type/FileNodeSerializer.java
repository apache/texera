package edu.uci.ics.texera.web.resource.dashboard.user.dataset.type;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

// this class is used to serialize the FileNode as JSON. So that FileNodes can be inspected by the frontend through JSON.
public class FileNodeSerializer extends StdSerializer<FileNode> {

  public FileNodeSerializer() {
    this(null);
  }

  public FileNodeSerializer(Class<FileNode> t) {
    super(t);
  }

  @Override
  public void serialize(FileNode value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("path", value.getRelativePath().toString());
    gen.writeBooleanField("isFile", value.isFile());
    if (value.isDirectory()) {
      gen.writeFieldName("children");
      gen.writeStartArray();
      for (FileNode child : value.getChildren()) {
        serialize(child, gen, provider); // Recursively serialize children
      }
      gen.writeEndArray();
    }
    gen.writeEndObject();
  }
}
