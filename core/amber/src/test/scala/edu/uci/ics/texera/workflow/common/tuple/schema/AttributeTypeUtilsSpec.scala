package edu.uci.ics.texera.workflow.common.tuple.schema

import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.{
  inferField,
  inferSchemaFromRows
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

  }

  test("types should get inferred correctly with one row") {
    val row: Array[Object] =
      Array("string", "1", "2020-01-02T00:05:56.000Z", "1.3", "213214124124124", "true")
    val rows: Iterator[Array[Object]] = Iterator(row)
    val attributeTypes = inferSchemaFromRows(rows)
    assert(attributeTypes(0) == STRING)
    assert(attributeTypes(1) == INTEGER)
    assert(attributeTypes(2) == TIMESTAMP)
    assert(attributeTypes(3) == DOUBLE)
    assert(attributeTypes(4) == LONG)
    assert(attributeTypes(5) == BOOLEAN)

  }

  test("types should get inferred correctly with multiple rows") {

    val rows: Iterator[Array[Object]] = Iterator(
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

}
