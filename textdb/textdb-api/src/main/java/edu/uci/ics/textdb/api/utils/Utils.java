package edu.uci.ics.textdb.api.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class Utils {
    
    /**
    *
    * @param schema
    * @about Creating a new schema object, and adding SPAN_LIST_ATTRIBUTE to
    *        the schema. SPAN_LIST_ATTRIBUTE is of type List
    */
   public static Schema createSpanSchema(Schema schema) {
       return addAttributeToSchema(schema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
   }

   /**
    * Add an attribute to an existing schema (if the attribute doesn't exist).
    * 
    * @param schema
    * @param attribute
    * @return new schema
    */
   public static Schema addAttributeToSchema(Schema schema, Attribute attribute) {
       if (schema.containsField(attribute.getAttributeName())) {
           return schema;
       }
       List<Attribute> attributes = new ArrayList<>(schema.getAttributes());
       attributes.add(attribute);
       Schema newSchema = new Schema(attributes.toArray(new Attribute[attributes.size()]));
       return newSchema;
   }
   
   /**
    * Removes one or more attributes from the schema and returns the new schema.
    * 
    * @param schema
    * @param attributeName
    * @return
    */
   public static Schema removeAttributeFromSchema(Schema schema, String... attributeName) {
       return new Schema(schema.getAttributes().stream()
               .filter(attr -> (! Arrays.asList(attributeName).contains(attr.getAttributeName())))
               .toArray(Attribute[]::new));
   }
   
   /**
    * Converts a list of attributes to a list of attribute names
    * 
    * @param attributeList, a list of attributes
    * @return a list of attribute names
    */
   public static List<String> getAttributeNames(List<Attribute> attributeList) {
       return attributeList.stream()
               .map(attr -> attr.getAttributeName())
               .collect(Collectors.toList());
   }
   
   /**
    * Converts a list of attributes to a list of attribute names
    * 
    * @param attributeList, a list of attributes
    * @return a list of attribute names
    */
   public static List<String> getAttributeNames(Attribute... attributeList) {
       return Arrays.asList(attributeList).stream()
               .map(attr -> attr.getAttributeName())
               .collect(Collectors.toList());
   }
   
   /**
    * Creates a new schema object, with "_ID" attribute added to the front.
    * If the schema already contains "_ID" attribute, returns the original schema.
    * 
    * @param schema
    * @return
    */
   public static Schema getSchemaWithID(Schema schema) {
       if (schema.containsField(SchemaConstants._ID)) {
           return schema;
       }
       
       List<Attribute> attributeList = new ArrayList<>();
       attributeList.add(SchemaConstants._ID_ATTRIBUTE);
       attributeList.addAll(schema.getAttributes());
       return new Schema(attributeList.stream().toArray(Attribute[]::new));      
   }
   
   /**
    * Remove one or more fields from each tuple in tupleList.
    * 
    * @param tupleList
    * @param removeFields
    * @return
    */
   public static List<Tuple> removeFields(List<Tuple> tupleList, String... removeFields) {
       List<Tuple> newTuples = tupleList.stream().map(tuple -> removeFields(tuple, removeFields))
               .collect(Collectors.toList());
       return newTuples;
   }
   
   /**
    * Remove one or more fields from a tuple.
    * 
    * @param tuple
    * @param removeFields
    * @return
    */
   public static Tuple removeFields(Tuple tuple, String... removeFields) {
       List<String> removeFieldList = Arrays.asList(removeFields);
       List<Integer> removedFeidsIndex = removeFieldList.stream()
               .map(attributeName -> tuple.getSchema().getIndex(attributeName)).collect(Collectors.toList());
       
       Attribute[] newAttrs = tuple.getSchema().getAttributes().stream()
               .filter(attr -> (! removeFieldList.contains(attr.getAttributeName()))).toArray(Attribute[]::new);
       Schema newSchema = new Schema(newAttrs);
       
       IField[] newFields = IntStream.range(0, tuple.getSchema().getAttributes().size())
           .filter(index -> (! removedFeidsIndex.contains(index)))
           .mapToObj(index -> tuple.getField(index)).toArray(IField[]::new);
       
       return new Tuple(newSchema, newFields);
   }

}
