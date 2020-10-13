package edu.uci.ics.texera.workflow.common.workflow

import scala.beans.BeanProperty

case class TexeraOperatorLink(
    @BeanProperty origin: String,
    @BeanProperty destination: String
)
