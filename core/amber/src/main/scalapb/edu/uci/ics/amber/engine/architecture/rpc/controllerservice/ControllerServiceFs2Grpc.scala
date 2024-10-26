package edu.uci.ics.amber.engine.architecture.rpc.controllerservice

import _root_.cats.syntax.all._

trait ControllerServiceFs2Grpc[F[_], A] {
  def retrieveWorkflowState(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse]
  def propagateChannelMarker(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse]
  def takeGlobalCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse]
  def debugCommand(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def evaluatePythonExpression(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse]
  def consoleMessageTriggered(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def portCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def startWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse]
  def resumeWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def pauseWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def workerStateUpdated(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def workerExecutionCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def linkWorkers(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def controllerInitiateQueryStatistics(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.QueryStatisticsRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
  def retryWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]
}

object ControllerServiceFs2Grpc extends _root_.fs2.grpc.GeneratedCompanion[ControllerServiceFs2Grpc] {
  
  def mkClient[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], channel: _root_.io.grpc.Channel, mkMetadata: A => F[_root_.io.grpc.Metadata], clientOptions: _root_.fs2.grpc.client.ClientOptions): ControllerServiceFs2Grpc[F, A] = new ControllerServiceFs2Grpc[F, A] {
    def retrieveWorkflowState(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RETRIEVE_WORKFLOW_STATE, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def propagateChannelMarker(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PROPAGATE_CHANNEL_MARKER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def takeGlobalCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_TAKE_GLOBAL_CHECKPOINT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def debugCommand(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_DEBUG_COMMAND, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def evaluatePythonExpression(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_EVALUATE_PYTHON_EXPRESSION, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def consoleMessageTriggered(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_CONSOLE_MESSAGE_TRIGGERED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def portCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PORT_COMPLETED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def startWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_START_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def resumeWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RESUME_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def pauseWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PAUSE_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def workerStateUpdated(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_WORKER_STATE_UPDATED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def workerExecutionCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_WORKER_EXECUTION_COMPLETED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def linkWorkers(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_LINK_WORKERS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def controllerInitiateQueryStatistics(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.QueryStatisticsRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_CONTROLLER_INITIATE_QUERY_STATISTICS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
    def retryWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, ctx: A): F[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RETRY_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCall(request, m))
      }
    }
  }
  
  protected def serviceBinding[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], serviceImpl: ControllerServiceFs2Grpc[F, A], mkCtx: _root_.io.grpc.Metadata => F[A], serverOptions: _root_.fs2.grpc.server.ServerOptions): _root_.io.grpc.ServerServiceDefinition = {
    _root_.io.grpc.ServerServiceDefinition
      .builder(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.SERVICE)
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RETRIEVE_WORKFLOW_STATE, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.retrieveWorkflowState(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PROPAGATE_CHANNEL_MARKER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.propagateChannelMarker(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_TAKE_GLOBAL_CHECKPOINT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.takeGlobalCheckpoint(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_DEBUG_COMMAND, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.debugCommand(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_EVALUATE_PYTHON_EXPRESSION, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.evaluatePythonExpression(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_CONSOLE_MESSAGE_TRIGGERED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.consoleMessageTriggered(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PORT_COMPLETED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.portCompleted(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_START_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.startWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RESUME_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.resumeWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_PAUSE_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.pauseWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_WORKER_STATE_UPDATED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.workerStateUpdated(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_WORKER_EXECUTION_COMPLETED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.workerExecutionCompleted(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_LINK_WORKERS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.linkWorkers(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_CONTROLLER_INITIATE_QUERY_STATISTICS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.QueryStatisticsRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.controllerInitiateQueryStatistics(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_RETRY_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCall[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn]((r, m) => mkCtx(m).flatMap(serviceImpl.retryWorkflow(r, _))))
      .build()
  }

}