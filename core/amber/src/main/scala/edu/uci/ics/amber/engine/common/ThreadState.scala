package edu.uci.ics.amber.engine.common

object ThreadState extends Enumeration {
  val Idle = Value("Idle")
  val Running = Value("Running")
  val Paused = Value("Paused")
  val LocalBreakpointTriggered = Value("LocalBreakpointTriggered")
  val Completed = Value("Completed")
}
