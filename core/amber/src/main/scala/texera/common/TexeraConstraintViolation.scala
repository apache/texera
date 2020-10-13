package texera.common

object TexeraConstraintViolation {}

case class TexeraConstraintViolation(
    message: String,
    propertyPath: String
)
