package edu.uci.ics.texera.workflow.operators.visualization.scatterplot

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.operators.flatmap.FlatMapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

/**
  * Scatterplot operator to visualize the result as a scatterplot
  */
object ScatterplotOpExec {

  /**
    *  longitude to spherical mercator in [0..1] range
    */
  private def lngX(lng: Double): Double = lng / 360 + 0.5

  /**
    *  latitude to spherical mercator in [0..1] range
    */
  private def latY(lat: Double): Double = {
    val sin = Math.sin(lat * Math.PI / 180)
    val y = 0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI
    if (y < 0) {
      0
    } else if (y > 1) {
      1
    } else {
      y
    }
  }
}
class ScatterplotOpExec(
    private val opDesc: ScatterplotOpDesc,
    private val operatorSchemaInfo: OperatorSchemaInfo
) extends FlatMapOpExec {
  this.setFlatMapFunc(this.process)
  private val pixelGrid: Option[Array[Array[Boolean]]] =
    if (opDesc.isGeometric) {
      Some(Array.ofDim[Boolean](Constants.MAX_RESOLUTION_ROWS, Constants.MAX_RESOLUTION_COLUMNS))
    } else {
      None
    }

  private def process(t: Tuple): Iterator[Tuple] = {

    if (opDesc.isGeometric) {
      val row_index = Math
        .floor(ScatterplotOpExec.lngX(t.getField(opDesc.xColumn)) * Constants.MAX_RESOLUTION_ROWS)
        .toInt
      val column_index = Math
        .floor(
          ScatterplotOpExec.latY(t.getField(opDesc.yColumn)) * Constants.MAX_RESOLUTION_COLUMNS
        )
        .toInt
      if (pixelGrid.get(row_index)(column_index)) {
        return Iterator()
      }
      pixelGrid.get(row_index)(column_index) = true
    }

    Iterator(
      Tuple
        .newBuilder(operatorSchemaInfo.outputSchemas(0))
        .addSequentially(Array(t.getField(opDesc.xColumn), t.getField(opDesc.yColumn)))
        .build
    )
  }

}
