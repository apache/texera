package edu.uci.ics.amber.core.tuple

import edu.uci.ics.amber.core.tuple.AttributeType._
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.{
  AttributeTypeException,
  inferField,
  inferSchemaFromRows,
  parseField
}
import org.scalatest.funsuite.AnyFunSuite

class AttributeTypeUtilsSpec extends AnyFunSuite {
  // Unit Test for Infer Schema

  test("type should get inferred correctly individually") {

    assert(inferField(" 1     \n\n") == INTEGER)
    assert(inferField(" 1.1\t") == DOUBLE)
    assert(inferField("1,111.1 ") == STRING)
    assert(inferField("k2068-10-29T18:43:15.000Z") == STRING)
    assert(inferField(" 12321321312321312312321321 ") == DOUBLE)
    assert(inferField(" 123,123,123,123,123,123,123.11") == STRING)
    assert(inferField(" 00\t") == INTEGER)
    assert(inferField("\t-.2 ") == DOUBLE)
    assert(inferField("\n False ") == BOOLEAN)
    assert(inferField("07/10/96 4:5 PM, PDT") == TIMESTAMP)
    assert(inferField("02/2/2020") == TIMESTAMP)
    assert(inferField("\n\n02/2/23    ") == TIMESTAMP)
    assert(inferField("   2023年8月7日   ") == TIMESTAMP)
    assert(
      inferField("2020-12-31T23:25:59.999Z") == TIMESTAMP
    ) // ISO format with milliseconds and UTC
    assert(inferField("2020-12-31T11:59:59+01:00") == TIMESTAMP) // ISO format with timezone offset
    assert(
      inferField("2020-12-31T11:59:59") == TIMESTAMP
    ) // ISO format without milliseconds and timezone
    assert(
      inferField("31/12/2020 23:59:59") == TIMESTAMP
    ) // European datetime format with slash separators
    assert(
      inferField("12/31/2020 11:59:59") == TIMESTAMP
    ) // US datetime format with slash separators
    assert(inferField("2020-12-31") == TIMESTAMP) // Common date format
    assert(inferField("31-Dec-2020") == TIMESTAMP) // Date format with three-letter month
    assert(
      inferField("Wednesday, 31-Dec-20 23:59:59 GMT") == TIMESTAMP
    ) // Verbose format with day and timezone
    assert(
      inferField("1 Jan 2020 05:30:00 GMT") == TIMESTAMP
    ) // Another verbose format with timezone
    assert(inferField("15-Aug-2020 20:20:20") == TIMESTAMP) // Day-Month-Year format with time
    assert(inferField("2020年12月31日 23:59") == TIMESTAMP) // East Asian date format with time
    assert(inferField("2020/12/31 23:59") == TIMESTAMP) // Alternate slash format with time

  }

  test("types should get inferred correctly with one row") {
    val row: Array[Any] =
      Array("string", "1", "2020-01-02T00:05:56.000Z", "1.3", "213214124124124", "true")
    val rows: Iterator[Array[Any]] = Iterator(row)
    val attributeTypes = inferSchemaFromRows(rows)
    assert(attributeTypes(0) == STRING)
    assert(attributeTypes(1) == INTEGER)
    assert(attributeTypes(2) == TIMESTAMP)
    assert(attributeTypes(3) == DOUBLE)
    assert(attributeTypes(4) == LONG)
    assert(attributeTypes(5) == BOOLEAN)

  }

  test("types should get inferred correctly with multiple rows") {

    val rows: Iterator[Array[Any]] = Iterator(
      Array("string", "1 ", "2020-01-02T00:05:56.000Z", "1.3 ", "9223372036854775807", "true"),
      Array("1932-09-06", "0 ", "1932-09-06T03:47:19Z", "9223.23", "-1", "false "),
      Array("", "-1", "1979-08-12T10:18:49Z", "-.11", "-9223372036854775808 ", "0"),
      Array("123,456,789", " -0", " 2023-6-7 8:9:38", " -9.32", "0", "1"),
      Array("92233720368547758072", "2147483647", "2023-06-27T08:09:38Z", ".1", "1", " TRUE"),
      Array("\n", "-2147483648", "2068-10-29T18:43:15.000Z ", " 100.00 ", "03685477", "FALSE")
    )
    val attributeTypes = inferSchemaFromRows(rows)
    assert(attributeTypes(0) == STRING)
    assert(attributeTypes(1) == INTEGER)
    assert(attributeTypes(2) == TIMESTAMP)
    assert(attributeTypes(3) == DOUBLE)
    assert(attributeTypes(4) == LONG)
    assert(attributeTypes(5) == BOOLEAN)

  }

  test("parseField correctly parses to INTEGER") {
    assert(parseField("123", AttributeType.INTEGER) == 123)
    assert(parseField("1,234", AttributeType.INTEGER, force = true) == 1234)
    assert(parseField(456, AttributeType.INTEGER) == 456)
    assert(parseField(123.45, AttributeType.INTEGER) == 123)
    assert(parseField(true, AttributeType.INTEGER) == 1)
    assert(parseField(false, AttributeType.INTEGER) == 0)
    assertThrows[AttributeTypeException] {
      parseField("invalid", AttributeType.INTEGER)
    }
    assertThrows[AttributeTypeException] {
      parseField("1,234", AttributeType.INTEGER)
    }
  }

  test("parseField correctly parses to LONG") {
    assert(parseField("1234567890", AttributeType.LONG) == 1234567890L)
    assert(parseField("1,234,567", AttributeType.LONG, force = true) == 1234567L)
    assert(parseField(12345L, AttributeType.LONG) == 12345L)
    assert(parseField(123.45, AttributeType.LONG) == 123L)
    assert(parseField(true, AttributeType.LONG) == 1L)
    assertThrows[AttributeTypeException] {
      parseField("invalid", AttributeType.LONG)
    }
    assertThrows[AttributeTypeException] {
      parseField("1,234,567", AttributeType.LONG)
    }
  }

  test("parseField correctly parses to DOUBLE") {
    assert(parseField("123.45", AttributeType.DOUBLE) == 123.45)
    assert(parseField(12345, AttributeType.DOUBLE) == 12345.0)
    assert(parseField(12345L, AttributeType.DOUBLE) == 12345.0)
    assert(parseField(true, AttributeType.DOUBLE) == 1.0)
    assertThrows[AttributeTypeException] {
      parseField("invalid", AttributeType.DOUBLE)
    }
  }

  test("parseField correctly parses to BOOLEAN") {
    assert(parseField("true", AttributeType.BOOLEAN) == true)
    assert(parseField("True", AttributeType.BOOLEAN) == true)
    assert(parseField("TRUE", AttributeType.BOOLEAN) == true)
    assert(parseField("false", AttributeType.BOOLEAN) == false)
    assert(parseField("False", AttributeType.BOOLEAN) == false)
    assert(parseField("FALSE", AttributeType.BOOLEAN) == false)
    assert(parseField("1", AttributeType.BOOLEAN) == true)
    assert(parseField("0", AttributeType.BOOLEAN) == false)
    assert(parseField(1, AttributeType.BOOLEAN) == true)
    assert(parseField(0, AttributeType.BOOLEAN) == false)
    assertThrows[AttributeTypeException] {
      parseField("invalid", AttributeType.BOOLEAN)
    }
  }

  test("parseField correctly parses to TIMESTAMP") {
    val timestamp =
      parseField("2023-11-13T10:15:30", AttributeType.TIMESTAMP).asInstanceOf[java.sql.Timestamp]
    assert(timestamp.toString == "2023-11-13 10:15:30.0")

    assert(
      parseField(1699820130000L, AttributeType.TIMESTAMP)
        .asInstanceOf[java.sql.Timestamp]
        .getTime == 1699820130000L
    )

    assertThrows[AttributeTypeException] {
      parseField("invalid", AttributeType.TIMESTAMP)
    }
  }

  test("parseField correctly parses to STRING") {
    assert(parseField(123, AttributeType.STRING) == "123")
    assert(parseField(123.45, AttributeType.STRING) == "123.45")
    assert(parseField(true, AttributeType.STRING) == "true")
  }

  test("parseField returns original value for BINARY and ANY") {
    val binaryData = Array[Byte](1, 2, 3)
    assert(parseField(binaryData, AttributeType.BINARY) == binaryData)
    assert(parseField("anything", AttributeType.ANY) == "anything")
  }

}
