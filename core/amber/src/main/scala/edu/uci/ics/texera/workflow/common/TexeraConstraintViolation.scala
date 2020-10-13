package edu.uci.ics.texera.workflow.common

object TexeraConstraintViolation {}

case class TexeraConstraintViolation(
    message: String,
    propertyPath: String
)
