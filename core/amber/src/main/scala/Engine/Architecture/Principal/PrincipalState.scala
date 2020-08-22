package Engine.Architecture.Principal

/*
Principal State Transition (view from controller side):

Normal case                                      If message dropped
UnInitialized -> Ready                      =>   ()                 // enforce controller check
Ready -> (Running | Pausing | Paused)       =>   (Completed)
Running -> (Pausing | Completed)            =>   (Paused)
Pausing -> (Paused | Completed)             =>   ()
Paused -> Resuming                          =>   (Running | Completed)
Resuming -> (Running | Ready)               =>   (Completed)
Completed -> ()                             =>   ()
 */

object PrincipalState extends Enumeration {
  type PrincipalState = Value

  val Uninitialized, Initializing, Ready, Running, Pausing, CollectingBreakpoints, Paused, Resuming,
      Completed = Value

  val ValidTransitions: Map[PrincipalState.Value, Set[PrincipalState.Value]] = Map(
    Uninitialized -> Set(Initializing),
    Initializing -> Set(Ready),
    Ready -> Set(Running, Paused, Pausing),
    Running -> Set(Pausing, Completed),
    Pausing -> Set(CollectingBreakpoints, Paused, Completed),
    CollectingBreakpoints -> Set(Paused),
    Paused -> Set(Resuming),
    Resuming -> Set(Running, Ready),
    Completed -> Set()
  )

  val SkippedTransitions: Map[PrincipalState.Value, Set[PrincipalState.Value]] = Map(
    Uninitialized -> Set(),
    Initializing -> Set(),
    Ready -> Set(Completed),
    Running -> Set(Paused),
    Pausing -> Set(),
    CollectingBreakpoints -> Set(),
    Paused -> Set(Running, Completed),
    Resuming -> Set(Completed),
    Completed -> Set()
  )
}
