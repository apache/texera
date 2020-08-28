package Engine.Operators.PythonUDF

import Engine.Common.AmberField.FieldType

object FieldTypeConverter {
  def convertType(javaField: FieldTypeInJava): FieldType.Value = javaField match {
      case FieldTypeInJava.SHORT      => FieldType.Short
      case FieldTypeInJava.INT        => FieldType.Int
      case FieldTypeInJava.BOOLEAN    => FieldType.Boolean
      case FieldTypeInJava.CHAR       => FieldType.Char
      case FieldTypeInJava.DOUBLE     => FieldType.Double
      case FieldTypeInJava.BYTE       => FieldType.Byte
      case FieldTypeInJava.FLOAT      => FieldType.Float
      case FieldTypeInJava.LONG       => FieldType.Long
      case FieldTypeInJava.STRING     => FieldType.String
      case FieldTypeInJava.BIGINT     => FieldType.BigInt
      case FieldTypeInJava.BIGDECIMAL => FieldType.BigDecimal
      case FieldTypeInJava.OTHER      => FieldType.Other
  }

  def convertType(javaFields: Array[FieldTypeInJava]): Array[FieldType.Value] = {
    val result = new Array[FieldType.Value](javaFields.length)
    var i = 0
    while (i < javaFields.length) {
      result(i) = convertType(javaFields(i))
      i += 1
    }
    result
  }
}
