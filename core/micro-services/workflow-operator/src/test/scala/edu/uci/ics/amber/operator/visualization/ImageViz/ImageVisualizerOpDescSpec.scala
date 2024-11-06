package edu.uci.ics.amber.operator.visualization.ImageViz

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class ImageVisualizerOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: ImageVisualizerOpDesc = _
  before {
    opDesc = new ImageVisualizerOpDesc()
  }

  it should "throw assertion error if BinaryContent is empty" in {
    assertThrows[NullPointerException] {
      opDesc.createBinaryData()
    }
  }
}
