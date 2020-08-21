package texera.common.schema

import scala.beans.BeanProperty

case class TexeraOperatorDescription
(
  @BeanProperty userFriendlyName: String,
  @BeanProperty operatorDescription: String,
  @BeanProperty operatorGroupName: String,
  @BeanProperty numInputPorts: Int,
  @BeanProperty numOutputPorts: Int
)
