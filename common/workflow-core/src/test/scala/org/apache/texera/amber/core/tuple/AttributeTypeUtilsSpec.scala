/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.core.tuple

import org.apache.texera.amber.core.tuple.AttributeType._
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.{
  AttributeTypeException,
  inferField,
  inferSchemaFromRows,
  parseField,
  compare,
  add,
  minValue,
  maxValue,
  zeroValue
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

  test("parseField correctly parses to BIG_OBJECT") {
    // Valid S3 URI strings are converted to BigObject
    val pointer1 = parseField("s3://bucket/path/to/object", AttributeType.BIG_OBJECT)
      .asInstanceOf[BigObject]
    assert(pointer1.getUri == "s3://bucket/path/to/object")
    assert(pointer1.getBucketName == "bucket")
    assert(pointer1.getObjectKey == "path/to/object")

    // Null input returns null
    assert(parseField(null, AttributeType.BIG_OBJECT) == null)
  }

  test("BIG_OBJECT type is preserved but never inferred from data") {
    // BIG_OBJECT remains BIG_OBJECT when passed as typeSoFar
    assert(inferField(AttributeType.BIG_OBJECT, "any-value") == AttributeType.BIG_OBJECT)
    assert(inferField(AttributeType.BIG_OBJECT, null) == AttributeType.BIG_OBJECT)

    // String data is inferred as STRING, never BIG_OBJECT
    assert(inferField("s3://bucket/path") == AttributeType.STRING)
  }

  test("compare correctly handles null values for different attribute types") {
    assert(compare(null, null, INTEGER) == 0)
    assert(compare(null, 10, INTEGER) < 0)
    assert(compare(10, null, INTEGER) > 0)
  }

  test("compare correctly orders numeric, boolean, timestamp, string and binary values") {
    assert(compare(1, 2, INTEGER) < 0)
    assert(compare(2, 1, INTEGER) > 0)
    assert(compare(5, 5, INTEGER) == 0)

    assert(compare(false, true, BOOLEAN) < 0)
    assert(compare(true, false, BOOLEAN) > 0)
    assert(compare(true, true, BOOLEAN) == 0)

    val earlierTimestamp = new java.sql.Timestamp(1000L)
    val laterTimestamp = new java.sql.Timestamp(2000L)
    assert(compare(earlierTimestamp, laterTimestamp, TIMESTAMP) < 0)
    assert(compare(laterTimestamp, earlierTimestamp, TIMESTAMP) > 0)

    assert(compare("apple", "banana", STRING) < 0)
    assert(compare("banana", "apple", STRING) > 0)
    assert(compare("same", "same", STRING) == 0)

    val firstBytes = Array[Byte](0, 1, 2)
    val secondBytes = Array[Byte](0, 2, 0)
    assert(compare(firstBytes, secondBytes, BINARY) < 0)
  }

  test("add correctly handles null values as identity for numeric types") {
    val integerZeroFromAdd = add(null, null, INTEGER).asInstanceOf[Int]
    assert(integerZeroFromAdd == 0)

    val rightOnlyResult =
      add(null, java.lang.Integer.valueOf(5), INTEGER).asInstanceOf[Int]
    assert(rightOnlyResult == 5)

    val leftOnlyResult =
      add(java.lang.Integer.valueOf(7), null, INTEGER).asInstanceOf[Int]
    assert(leftOnlyResult == 7)
  }

  test("add correctly adds integer, long, double and timestamp values") {
    val integerSum =
      add(java.lang.Integer.valueOf(3), java.lang.Integer.valueOf(4), INTEGER)
        .asInstanceOf[Int]
    assert(integerSum == 7)

    val longSum =
      add(java.lang.Long.valueOf(10L), java.lang.Long.valueOf(5L), LONG)
        .asInstanceOf[Long]
    assert(longSum == 15L)

    val doubleSum =
      add(java.lang.Double.valueOf(1.5), java.lang.Double.valueOf(2.5), DOUBLE)
        .asInstanceOf[Double]
    assert(doubleSum == 4.0)

    val firstTimestamp = new java.sql.Timestamp(1000L)
    val secondTimestamp = new java.sql.Timestamp(2500L)
    val timestampSum =
      add(firstTimestamp, secondTimestamp, TIMESTAMP).asInstanceOf[java.sql.Timestamp]
    assert(timestampSum.getTime == 3500L)
  }

  test("zeroValue returns correct numeric and timestamp identity values") {
    val integerZero = zeroValue(INTEGER).asInstanceOf[Int]
    val longZero = zeroValue(LONG).asInstanceOf[Long]
    val doubleZero = zeroValue(DOUBLE).asInstanceOf[Double]
    val timestampZero = zeroValue(TIMESTAMP).asInstanceOf[java.sql.Timestamp]

    assert(integerZero == 0)
    assert(longZero == 0L)
    assert(doubleZero == 0.0d)
    assert(timestampZero.getTime == 0L)
  }

  test("zeroValue returns empty binary array and fails for unsupported types") {
    val binaryZero = zeroValue(BINARY).asInstanceOf[Array[Byte]]
    assert(binaryZero.isEmpty)

    assertThrows[UnsupportedOperationException] {
      zeroValue(STRING)
    }
  }

  test("maxValue returns correct maximum numeric bounds") {
    val integerMax = maxValue(INTEGER).asInstanceOf[Int]
    val longMax = maxValue(LONG).asInstanceOf[Long]
    val doubleMax = maxValue(DOUBLE).asInstanceOf[Double]

    assert(integerMax == Int.MaxValue)
    assert(longMax == Long.MaxValue)
    assert(doubleMax == Double.MaxValue)
  }

  test("maxValue returns maximum timestamp and fails for unsupported types") {
    val timestampMax = maxValue(TIMESTAMP).asInstanceOf[java.sql.Timestamp]
    assert(timestampMax.getTime == Long.MaxValue)

    assertThrows[UnsupportedOperationException] {
      maxValue(BOOLEAN)
    }
  }

  test("minValue returns correct minimum numeric bounds") {
    val integerMin = minValue(INTEGER).asInstanceOf[Int]
    val longMin = minValue(LONG).asInstanceOf[Long]
    val doubleMin = minValue(DOUBLE).asInstanceOf[Double]

    assert(integerMin == Int.MinValue)
    assert(longMin == Long.MinValue)
    assert(doubleMin == java.lang.Double.MIN_VALUE)
  }

  test("minValue returns timestamp epoch and empty binary array, and fails for unsupported types") {
    val timestampMin = minValue(TIMESTAMP).asInstanceOf[java.sql.Timestamp]
    val binaryMin = minValue(BINARY).asInstanceOf[Array[Byte]]

    assert(timestampMin.getTime == 0L)

    assert(binaryMin.isEmpty)

    assertThrows[UnsupportedOperationException] {
      minValue(STRING)
    }
  }
}
