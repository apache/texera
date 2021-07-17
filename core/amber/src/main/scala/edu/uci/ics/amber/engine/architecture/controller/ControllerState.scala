package edu.uci.ics.amber.engine.architecture.controller

/*
CONTROLLER State Transition (view from controller side):

Normal case                                      If message dropped
UnInitialized -> Ready                      =>   ()
Ready -> (Running | Pausing | Paused)       =>   (Completed)
Running -> (Pausing | Completed)            =>   (Paused)
Pausing -> (Paused | Completed)             =>   ()
Paused -> Resuming                          =>   (Running | Completed)
Resuming -> (Running | Ready)               =>   (Completed)
Completed -> ()                             =>   ()
 */

object ControllerState extends Enumeration {
  val Uninitialized, Initializing, Ready, Running, Pausing, Paused, Resuming, Completed = Value

  val ValidTransitions: Map[ControllerState.Value, Set[ControllerState.Value]] = Map(
    Uninitialized -> Set(Ready),
    Initializing -> Set(Ready),
    Ready -> Set(Running, Paused, Pausing),
    Running -> Set(Pausing, Completed),
    Pausing -> Set(Paused, Completed),
    Paused -> Set(Resuming),
    Resuming -> Set(Running, Ready),
    Completed -> Set()
  )

  val SkippedTransitions: Map[ControllerState.Value, Set[ControllerState.Value]] = Map(
    Uninitialized -> Set(),
    Initializing -> Set(),
    Ready -> Set(Completed),
    Running -> Set(Paused),
    Pausing -> Set(),
    Paused -> Set(Running, Completed),
    Resuming -> Set(Completed),
    Completed -> Set()
  )
}
