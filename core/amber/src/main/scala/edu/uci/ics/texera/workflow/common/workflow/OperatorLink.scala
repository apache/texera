package edu.uci.ics.texera.workflow.common.workflow

import scala.beans.BeanProperty

case class OperatorLink(
    @BeanProperty origin: String,
    @BeanProperty destination: String
)
