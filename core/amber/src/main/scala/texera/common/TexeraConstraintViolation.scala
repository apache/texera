package texera.common

import scala.beans.BeanProperty

object TexeraConstraintViolation {}

case class TexeraConstraintViolation(
    @BeanProperty message: String,
    @BeanProperty propertyPath: String
)
