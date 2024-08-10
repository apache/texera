package edu.uci.ics.texera.workflow.common

sealed trait Marker

final case class EndOfUpstream() extends Marker
