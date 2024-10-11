package edu.uci.ics.amber.engine.architecture.rpc.controllerservice

import _root_.cats.syntax.all._

trait ControllerServiceFs2GrpcTrailers[F[_], A] {
  def sendRetrieveWorkflowState(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetrieveWorkflowStateRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse, _root_.io.grpc.Metadata)]
  def sendPropagateChannelMarker(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse, _root_.io.grpc.Metadata)]
  def sendTakeGlobalCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse, _root_.io.grpc.Metadata)]
  def sendDebugCommand(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendEvaluatePythonExpression(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse, _root_.io.grpc.Metadata)]
  def sendModifyLogic(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ModifyLogicRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendRetryWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendConsoleMessageTriggered(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendPortCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendStartWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.StartWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse, _root_.io.grpc.Metadata)]
  def sendResumeWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ResumeWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendPauseWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PauseWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendWorkerStateUpdated(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendWorkerExecutionCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerExecutionCompletedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def sendLinkWorkers(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
}

object ControllerServiceFs2GrpcTrailers extends _root_.fs2.grpc.GeneratedCompanion[ControllerServiceFs2GrpcTrailers] {
  
  def serviceDescriptor: _root_.io.grpc.ServiceDescriptor = edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.SERVICE
  
  def mkClient[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], channel: _root_.io.grpc.Channel, mkMetadata: A => F[_root_.io.grpc.Metadata], clientOptions: _root_.fs2.grpc.client.ClientOptions): ControllerServiceFs2GrpcTrailers[F, A] = new ControllerServiceFs2GrpcTrailers[F, A] {
    def sendRetrieveWorkflowState(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetrieveWorkflowStateRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RETRIEVE_WORKFLOW_STATE, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendPropagateChannelMarker(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PROPAGATE_CHANNEL_MARKER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendTakeGlobalCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_TAKE_GLOBAL_CHECKPOINT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendDebugCommand(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_DEBUG_COMMAND, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendEvaluatePythonExpression(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_EVALUATE_PYTHON_EXPRESSION, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendModifyLogic(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ModifyLogicRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_MODIFY_LOGIC, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendRetryWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RETRY_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendConsoleMessageTriggered(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_CONSOLE_MESSAGE_TRIGGERED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendPortCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PORT_COMPLETED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendStartWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.StartWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_START_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendResumeWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ResumeWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RESUME_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendPauseWorkflow(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PauseWorkflowRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PAUSE_WORKFLOW, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendWorkerStateUpdated(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_WORKER_STATE_UPDATED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendWorkerExecutionCompleted(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerExecutionCompletedRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_WORKER_EXECUTION_COMPLETED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendLinkWorkers(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_LINK_WORKERS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
  }
  
  protected def serviceBinding[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], serviceImpl: ControllerServiceFs2GrpcTrailers[F, A], mkCtx: _root_.io.grpc.Metadata => F[A], serverOptions: _root_.fs2.grpc.server.ServerOptions): _root_.io.grpc.ServerServiceDefinition = {
    _root_.io.grpc.ServerServiceDefinition
      .builder(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.SERVICE)
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RETRIEVE_WORKFLOW_STATE, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetrieveWorkflowStateRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.RetrieveWorkflowStateResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendRetrieveWorkflowState(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PROPAGATE_CHANNEL_MARKER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PropagateChannelMarkerRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.PropagateChannelMarkerResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPropagateChannelMarker(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_TAKE_GLOBAL_CHECKPOINT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.TakeGlobalCheckpointRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendTakeGlobalCheckpoint(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_DEBUG_COMMAND, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.DebugCommandRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendDebugCommand(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_EVALUATE_PYTHON_EXPRESSION, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EvaluatePythonExpressionRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendEvaluatePythonExpression(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_MODIFY_LOGIC, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ModifyLogicRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendModifyLogic(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RETRY_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.RetryWorkflowRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendRetryWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_CONSOLE_MESSAGE_TRIGGERED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageTriggeredRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendConsoleMessageTriggered(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PORT_COMPLETED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PortCompletedRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPortCompleted(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_START_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.StartWorkflowRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendStartWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_RESUME_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ResumeWorkflowRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendResumeWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PAUSE_WORKFLOW, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PauseWorkflowRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPauseWorkflow(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_WORKER_STATE_UPDATED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerStateUpdatedRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendWorkerStateUpdated(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_WORKER_EXECUTION_COMPLETED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.WorkerExecutionCompletedRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendWorkerExecutionCompleted(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_LINK_WORKERS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.LinkWorkersRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.sendLinkWorkers(r, _))))
      .build()
  }

}