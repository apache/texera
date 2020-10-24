package edu.uci.ics.texera.workflow.common

object ConstraintViolation {}

case class ConstraintViolation(
    message: String,
    propertyPath: String
)
