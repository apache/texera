package edu.uci.ics.texera.workflow.operators.visualization.barChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class BarChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: BarChartOpDesc = _

  before {
    opDesc = new BarChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "list titles of axes in the python code" in {
    opDesc.fields = "geo.state_name"
    opDesc.value = "person.count"
    val temp = opDesc.manipulateTable()
    assert(temp.contains("geo.state_name"))
    assert(temp.contains("person.count"))
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

}
