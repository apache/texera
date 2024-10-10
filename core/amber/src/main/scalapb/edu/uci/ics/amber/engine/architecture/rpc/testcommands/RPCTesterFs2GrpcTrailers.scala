package edu.uci.ics.amber.engine.architecture.rpc.testcommands

import _root_.cats.syntax.all._

trait RPCTesterFs2GrpcTrailers[F[_], A] {
  def sendPing(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Ping, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)]
  def sendPong(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pong, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)]
  def sendNested(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Nested, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendPass(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pass, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendErrorCommand(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.ErrorCommand, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendRecursion(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Recursion, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendCollect(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Collect, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendGenerateNumber(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.GenerateNumber, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)]
  def sendMultiCall(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.MultiCall, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)]
  def sendChain(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Chain, ctx: A): F[(edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity, _root_.io.grpc.Metadata)]
}

object RPCTesterFs2GrpcTrailers extends _root_.fs2.grpc.GeneratedCompanion[RPCTesterFs2GrpcTrailers] {
  
  def serviceDescriptor: _root_.io.grpc.ServiceDescriptor = edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.SERVICE
  
  def mkClient[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], channel: _root_.io.grpc.Channel, mkMetadata: A => F[_root_.io.grpc.Metadata], clientOptions: _root_.fs2.grpc.client.ClientOptions): RPCTesterFs2GrpcTrailers[F, A] = new RPCTesterFs2GrpcTrailers[F, A] {
    def sendPing(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Ping, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PING, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendPong(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pong, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PONG, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendNested(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Nested, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_NESTED, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendPass(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pass, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PASS, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendErrorCommand(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.ErrorCommand, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_ERROR_COMMAND, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendRecursion(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Recursion, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_RECURSION, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendCollect(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Collect, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_COLLECT, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendGenerateNumber(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.GenerateNumber, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_GENERATE_NUMBER, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendMultiCall(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.MultiCall, ctx: A): F[(edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_MULTI_CALL, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
    def sendChain(request: edu.uci.ics.amber.engine.architecture.rpc.testcommands.Chain, ctx: A): F[(edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity, _root_.io.grpc.Metadata)] = {
      mkMetadata(ctx).flatMap { m =>
        _root_.fs2.grpc.client.Fs2ClientCall[F](channel, edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_CHAIN, dispatcher, clientOptions).flatMap(_.unaryToUnaryCallTrailers(request, m))
      }
    }
  }
  
  protected def serviceBinding[F[_]: _root_.cats.effect.Async, A](dispatcher: _root_.cats.effect.std.Dispatcher[F], serviceImpl: RPCTesterFs2GrpcTrailers[F, A], mkCtx: _root_.io.grpc.Metadata => F[A], serverOptions: _root_.fs2.grpc.server.ServerOptions): _root_.io.grpc.ServerServiceDefinition = {
    _root_.io.grpc.ServerServiceDefinition
      .builder(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.SERVICE)
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PING, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Ping, edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPing(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PONG, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pong, edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPong(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_NESTED, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Nested, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendNested(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_PASS, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Pass, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendPass(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_ERROR_COMMAND, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.ErrorCommand, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendErrorCommand(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_RECURSION, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Recursion, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendRecursion(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_COLLECT, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Collect, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendCollect(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_GENERATE_NUMBER, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.GenerateNumber, edu.uci.ics.amber.engine.architecture.rpc.testcommands.IntResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendGenerateNumber(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_MULTI_CALL, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.MultiCall, edu.uci.ics.amber.engine.architecture.rpc.testcommands.StringResponse]((r, m) => mkCtx(m).flatMap(serviceImpl.sendMultiCall(r, _))))
      .addMethod(edu.uci.ics.amber.engine.architecture.rpc.testcommands.RPCTesterGrpc.METHOD_SEND_CHAIN, _root_.fs2.grpc.server.Fs2ServerCallHandler[F](dispatcher, serverOptions).unaryToUnaryCallTrailers[edu.uci.ics.amber.engine.architecture.rpc.testcommands.Chain, edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity]((r, m) => mkCtx(m).flatMap(serviceImpl.sendChain(r, _))))
      .build()
  }

}