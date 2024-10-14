package edu.uci.ics.amber.engine.architecture.rpc.workerservice

import _root_.cats.syntax.all._

trait WorkerServiceFs2GrpcTrailers[F[_], A] {
  def addInputChannel(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddInputChannelRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def addPartitioning(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddPartitioningRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def assignPort(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AssignPortRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def finalizeCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.FinalizeCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.FinalizeCheckpointResponse, _root_.io.grpc.Metadata)]
  def flushNetworkBuffer(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def initializeExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def openExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def pauseWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)]
  def prepareCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PrepareCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def queryStatistics(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse, _root_.io.grpc.Metadata)]
  def resumeWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)]
  def retrieveState(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def startWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)]
  def updateExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateExecutorRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
  def updateMultipleExecutors(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateMultipleExecutorsRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)]
}

object WorkerServiceFs2GrpcTrailers extends _root_.fs2.grpc.GeneratedCompanion[WorkerServiceFs2GrpcTrailers] {
  
  def serviceDescriptor: _root_.io.grpc.ServiceDescriptor = edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.SERVICE
  
  def mkClient[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], channel: _root_.io.grpc.Channel, mkMetadata: A => F[_root_.io.grpc.Metadata], clientOptions: _root_.fs2.grpc.client.ClientOptions): WorkerServiceFs2GrpcTrailers[F, A] = new WorkerServiceFs2GrpcTrailers[F, A] {
    def addInputChannel(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddInputChannelRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ADD_INPUT_CHANNEL, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def addPartitioning(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddPartitioningRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ADD_PARTITIONING, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def assignPort(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AssignPortRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ASSIGN_PORT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def finalizeCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.FinalizeCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.FinalizeCheckpointResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FINALIZE_CHECKPOINT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def flushNetworkBuffer(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FLUSH_NETWORK_BUFFER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def initializeExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_INITIALIZE_EXECUTOR, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def openExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_OPEN_EXECUTOR, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def pauseWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PAUSE_WORKER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def prepareCheckpoint(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PrepareCheckpointRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PREPARE_CHECKPOINT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def queryStatistics(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def resumeWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RESUME_WORKER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def retrieveState(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def startWorker(request: edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_START_WORKER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def updateExecutor(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateExecutorRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_EXECUTOR, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def updateMultipleExecutors(request: edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateMultipleExecutorsRequest, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_MULTIPLE_EXECUTORS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
  }
  
  protected def serviceBinding[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], serviceImpl: WorkerServiceFs2GrpcTrailers[F, A], mkCtx: _root_.io.grpc.Metadata => F[A], serverOptions: _root_.fs2.grpc.server.ServerOptions): _root_.io.grpc.ServerServiceDefinition = {
    _root_.io.grpc.ServerServiceDefinition
      .builder(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.SERVICE)
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ADD_INPUT_CHANNEL, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddInputChannelRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.addInputChannel(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ADD_PARTITIONING, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AddPartitioningRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.addPartitioning(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_ASSIGN_PORT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AssignPortRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.assignPort(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FINALIZE_CHECKPOINT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.FinalizeCheckpointRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.FinalizeCheckpointResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.finalizeCheckpoint(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FLUSH_NETWORK_BUFFER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.flushNetworkBuffer(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_INITIALIZE_EXECUTOR, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.initializeExecutor(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_OPEN_EXECUTOR, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.openExecutor(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PAUSE_WORKER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.pauseWorker(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PREPARE_CHECKPOINT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.PrepareCheckpointRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.prepareCheckpoint(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.queryStatistics(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RESUME_WORKER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.resumeWorker(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.retrieveState(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_START_WORKER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.startWorker(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_EXECUTOR, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateExecutorRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.updateExecutor(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_MULTIPLE_EXECUTORS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.controlcommands.UpdateMultipleExecutorsRequest, edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty]((r, m) => mkCtx(m).flatMap(serviceImpl.updateMultipleExecutors(r, _))))
      .build()
  }

}