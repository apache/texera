package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.asJavaIterableConverter

class CartesianProductOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val leftPort: Int = 0
  val rightPort: Int = 1

  var opDesc: CartesianProductOpDesc = _
  var opExec: CartesianProductOpExec = _

  def generate_tuple(schema: Schema, value: Option[Int]): Tuple = {
    Tuple
      .newBuilder(schema)
      .addSequentially(
        (1 to schema.getAttributeNamesScala.length).map(_ => value.map(_.toString).orNull).toArray
      )
      .build()
  }

  def generate_schema(
      base_name: String,
      num_attributes: Int = 1,
      append_num: Boolean = true
  ): Schema = {
    val attrs: java.lang.Iterable[Attribute] = Range
      .inclusive(1, num_attributes)
      .map(num =>
        new Attribute(base_name + (if (append_num) "#@" + num else ""), AttributeType.STRING)
      )
      .asJava
    Schema.newBuilder().add(attrs).build()
  }

  before {
    opDesc = new CartesianProductOpDesc()
  }

  it should "work with basic two input streams with no duplicate attribute names" in {
    val numLeftSchemaAttributes: Int = 3
    val numRightSchemaAttributes: Int = 3
    val numLeftTuples: Int = 5
    val numRightTuples: Int = 5

    val leftSchema = generate_schema("left", numLeftSchemaAttributes)
    val rightSchema = generate_schema("right", numRightSchemaAttributes)
    val outputSchema = opDesc.getOutputSchema(Array(leftSchema, rightSchema))
    opExec = new CartesianProductOpExec(leftSchema, rightSchema, outputSchema)

    opExec.open()
    // process 5 left tuples
    (1 to numLeftTuples).map(value => {
      assert(
        opExec
          .processTexeraTuple(Left(generate_tuple(leftSchema, Some(value))), leftPort, null, null)
          .isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), leftPort, null, null).isEmpty)

    // process 5 right tuples
    val outputTuples: List[Tuple] = (numLeftTuples + 1 to numLeftTuples + numRightTuples)
      .map(value =>
        opExec
          .processTexeraTuple(Left(generate_tuple(rightSchema, Some(value))), rightPort, null, null)
      )
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList
    assert(opExec.processTexeraTuple(Right(InputExhausted()), rightPort, null, null).isEmpty)

    // verify correct output size
    assert(outputTuples.size == numLeftTuples * numRightTuples)
    assert(
      outputTuples.head.getSchema.getAttributeNames
        .size() == numLeftSchemaAttributes + numRightSchemaAttributes
    )

    opExec.close()
  }

  it should "work with basic two input streams with duplicate attribute names" in {
    val numLeftSchemaAttributes: Int = 5
    val numRightSchemaAttributes: Int = 7
    val numLeftTuples: Int = 4
    val numRightTuples: Int = 3

    val duplicateAttribute: Attribute = new Attribute("left", AttributeType.STRING)
    val leftSchema = Schema
      .newBuilder()
      .add(generate_schema("left", numLeftSchemaAttributes - 1))
      .add(duplicateAttribute)
      .build()
    val rightSchema = Schema
      .newBuilder()
      .add(generate_schema("right", numRightSchemaAttributes - 1))
      .add(duplicateAttribute)
      .build()
    val inputSchemas = Array(leftSchema, rightSchema)
    val outputSchema = opDesc.getOutputSchema(inputSchemas)

    // verify output schema is as expected & has no duplicates
    assert(
      outputSchema.getAttributeNamesScala.toSet.size == outputSchema.getAttributeNamesScala.size
    ) // no duplicates in output Schema
    // check left tuple attributes name remain same
    (0 until numLeftSchemaAttributes).map(index =>
      assert(
        leftSchema.getAttributeNamesScala
          .apply(index)
          .equals(outputSchema.getAttributeNamesScala.apply(index))
      )
    )
    // check right tuple attributes without duplicate names are handled
    (0 until numRightSchemaAttributes - 1).map(index =>
      assert(
        rightSchema.getAttributeNamesScala
          .apply(index)
          .equals(outputSchema.getAttributeNamesScala.apply(numLeftSchemaAttributes + index))
      )
    )
    // check right tuple attribute with duplicate name is handled
    val expectedAttrName: String = rightSchema.getAttributeNamesScala.apply(
      numRightSchemaAttributes - 1
    ) + "#@" + numLeftSchemaAttributes
    assert(
      expectedAttrName.equals(
        outputSchema.getAttributeNamesScala.apply(
          numLeftSchemaAttributes + numRightSchemaAttributes - 1
        )
      )
    )

    opExec = new CartesianProductOpExec(leftSchema, rightSchema, outputSchema)
    opExec.open()
    // process 4 left tuples
    (1 to numLeftTuples).map(value => {
      assert(
        opExec
          .processTexeraTuple(Left(generate_tuple(leftSchema, Some(value))), leftPort, null, null)
          .isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), leftPort, null, null).isEmpty)

    // process 3 right tuples
    val outputTuples: List[Tuple] = (numLeftTuples + 1 to numLeftTuples + numRightTuples)
      .map(value =>
        opExec
          .processTexeraTuple(Left(generate_tuple(rightSchema, Some(value))), rightPort, null, null)
      )
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList
    assert(opExec.processTexeraTuple(Right(InputExhausted()), rightPort, null, null).isEmpty)

    // verify correct output size
    assert(outputTuples.size == numLeftTuples * numRightTuples)
    assert(
      outputTuples.head.getSchema.getAttributeNames
        .size() == numLeftSchemaAttributes + numRightSchemaAttributes
    )
    opExec.close()
  }
}
