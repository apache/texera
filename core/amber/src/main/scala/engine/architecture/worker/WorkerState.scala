package engine.architecture.worker

/*
Worker State Transition (view from principal side):

Normal case                           If message dropped
Uninitialized -> Ready           =>   ()                   //enforce principal check
Ready -> (Running | Paused)      =>   (Completed)
Running -> (Completed | Paused)  =>   ()
Paused -> (Running | Ready)      =>   (Completed)
Completed -> ()                  =>   ()

 */

object WorkerState extends Enumeration {
  val Uninitialized, Ready, Running, Pausing, Paused, LocalBreakpointTriggered, Completed = Value

  val ValidTransitions: Map[WorkerState.Value, Set[WorkerState.Value]] = Map(
    Uninitialized -> Set(Ready),
    Ready -> Set(Running, Paused),
    Running -> Set(Completed, Pausing, Paused, LocalBreakpointTriggered),
    LocalBreakpointTriggered -> Set(Paused),
    Pausing -> Set(Paused),
    Paused -> Set(LocalBreakpointTriggered, Running, Ready),
    Completed -> Set()
  )

  val SkippedTransitions: Map[WorkerState.Value, Set[WorkerState.Value]] = Map(
    Uninitialized -> Set(),
    Ready -> Set(Completed),
    Running -> Set(),
    Pausing -> Set(),
    LocalBreakpointTriggered -> Set(),
    Paused -> Set(Completed),
    Completed -> Set()
  )

}
