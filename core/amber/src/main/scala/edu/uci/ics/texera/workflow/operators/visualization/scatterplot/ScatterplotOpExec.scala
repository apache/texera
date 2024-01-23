package edu.uci.ics.texera.workflow.operators.visualization.scatterplot

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.workflow.common.operators.flatmap.FlatMapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

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
    private val opDesc: ScatterplotOpDesc
) extends FlatMapOpExec {
  this.setFlatMapFunc(this.process)
  val outputSchema: Schema = opDesc.operatorInfo.outputPorts
    .map(outputPort => opDesc.outputPortToSchemaMapping(outputPort.id))
    .head

  private val MAX_RESOLUTION_ROWS = AmberConfig.MAX_RESOLUTION_ROWS
  private val MAX_RESOLUTION_COLUMNS = AmberConfig.MAX_RESOLUTION_COLUMNS

  private val pixelGrid: Option[Array[Array[Boolean]]] =
    if (opDesc.isGeometric) {
      // maintain a pixel grid to cache the shown points.
      // depending on the resolution, it will only show one point per grid.
      Some(Array.ofDim[Boolean](MAX_RESOLUTION_ROWS, MAX_RESOLUTION_COLUMNS))
    } else {
      None
    }

  private def process(t: Tuple): Iterator[Tuple] = {

    if (opDesc.isGeometric) {
      val row_index = Math
        .floor(ScatterplotOpExec.lngX(t.getField(opDesc.xColumn)) * MAX_RESOLUTION_ROWS)
        .toInt
      val column_index = Math
        .floor(
          ScatterplotOpExec.latY(t.getField(opDesc.yColumn)) * MAX_RESOLUTION_COLUMNS
        )
        .toInt
      if (pixelGrid.get(row_index)(column_index)) {
        // ignore the point as another point on the same grid is shown already.
        return Iterator()
      }
      pixelGrid.get(row_index)(column_index) = true
    }

    Iterator(
      Tuple
        .newBuilder(outputSchema)
        .addSequentially(Array(t.getField(opDesc.xColumn), t.getField(opDesc.yColumn)))
        .build
    )
  }

}
