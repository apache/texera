package edu.uci.ics.texera.unit
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.projection.ProjectionOpExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
class ProjectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tuple: Tuple = Tuple
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()
  var projectionOpExec: ProjectionOpExec = _
  before {
    projectionOpExec = new ProjectionOpExec(List())
  }

  it should "open" in {

    projectionOpExec.attributes ++= List("field1", "field2")
    projectionOpExec.open()

  }

  it should "process Tuple" in {

    projectionOpExec.attributes ++= List("field1", "field2")

    projectionOpExec.open()

    val processedTuple = projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.length() == 2)
    assert(processedTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(processedTuple.getField("field2").asInstanceOf[Int] == 1)
    assert(processedTuple.get(0) == "hello")
    assert(processedTuple.get(1) == 1)
  }

  it should "process Tuple with different order" in {

    projectionOpExec.attributes ++= List("field3", "field1")

    projectionOpExec.open()

    val processedTuple = projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.length() == 2)
    assert(processedTuple.getField("field3").asInstanceOf[Boolean])
    assert(processedTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(processedTuple.get(0) == true)
    assert(processedTuple.get(1) == "hello")
  }
  it should "raise RuntimeException on non-existing fields" in {
    projectionOpExec.attributes ++= List("field---5", "field---6")
    assertThrows[RuntimeException] {
      projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    }

  }

  it should "raise IllegalArgumentException on empty attributes" in {

    assertThrows[IllegalArgumentException] {
      projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    }

  }
}
