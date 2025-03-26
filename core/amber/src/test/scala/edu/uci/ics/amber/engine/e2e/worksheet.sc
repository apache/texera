client
  .registerCallback[ExecutionStateUpdate](evt => {
    if (evt.state == COMPLETED) {