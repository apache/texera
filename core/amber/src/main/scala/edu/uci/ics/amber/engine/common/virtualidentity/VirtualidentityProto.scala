package edu.uci.ics.amber.engine.common.virtualidentity

object VirtualidentityProto extends _root_.scalapb.GeneratedFileObject {
  lazy val dependencies: Seq[_root_.scalapb.GeneratedFileObject] = Seq.empty
  lazy val messagesCompanions
      : Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] =
    Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]](
      edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentityMessage,
      edu.uci.ics.amber.engine.common.virtualidentity.WorkerActorVirtualIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.ControllerVirtualIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.SelfVirtualIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.ClientVirtualIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.LayerIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity,
      edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
    )
  private lazy val ProtoBytes: _root_.scala.Array[Byte] =
    scalapb.Encoding.fromBase64(
      scala.collection.immutable
        .Seq(
          """CjVlZHUvdWNpL2ljcy9hbWJlci9lbmdpbmUvY29tbW9uL3ZpcnR1YWxpZGVudGl0eS5wcm90bxIfZWR1LnVjaS5pY3MuYW1iZ
  XIuZW5naW5lLmNvbW1vbiL2BAoUQWN0b3JWaXJ0dWFsSWRlbnRpdHkSngEKGndvcmtlckFjdG9yVmlydHVhbElkZW50aXR5GAEgA
  SgLMjsuZWR1LnVjaS5pY3MuYW1iZXIuZW5naW5lLmNvbW1vbi5Xb3JrZXJBY3RvclZpcnR1YWxJZGVudGl0eUIf4j8cEhp3b3JrZ
  XJBY3RvclZpcnR1YWxJZGVudGl0eUgAUhp3b3JrZXJBY3RvclZpcnR1YWxJZGVudGl0eRKaAQoZY29udHJvbGxlclZpcnR1YWxJZ
  GVudGl0eRgCIAEoCzI6LmVkdS51Y2kuaWNzLmFtYmVyLmVuZ2luZS5jb21tb24uQ29udHJvbGxlclZpcnR1YWxJZGVudGl0eUIe4
  j8bEhljb250cm9sbGVyVmlydHVhbElkZW50aXR5SABSGWNvbnRyb2xsZXJWaXJ0dWFsSWRlbnRpdHkSggEKE3NlbGZWaXJ0dWFsS
  WRlbnRpdHkYAyABKAsyNC5lZHUudWNpLmljcy5hbWJlci5lbmdpbmUuY29tbW9uLlNlbGZWaXJ0dWFsSWRlbnRpdHlCGOI/FRITc
  2VsZlZpcnR1YWxJZGVudGl0eUgAUhNzZWxmVmlydHVhbElkZW50aXR5EooBChVjbGllbnRWaXJ0dWFsSWRlbnRpdHkYBCABKAsyN
  i5lZHUudWNpLmljcy5hbWJlci5lbmdpbmUuY29tbW9uLkNsaWVudFZpcnR1YWxJZGVudGl0eUIa4j8XEhVjbGllbnRWaXJ0dWFsS
  WRlbnRpdHlIAFIVY2xpZW50VmlydHVhbElkZW50aXR5Qg4KDHNlYWxlZF92YWx1ZSI7ChpXb3JrZXJBY3RvclZpcnR1YWxJZGVud
  Gl0eRIdCgRuYW1lGAEgAigJQgniPwYSBG5hbWVSBG5hbWUiGwoZQ29udHJvbGxlclZpcnR1YWxJZGVudGl0eSIVChNTZWxmVmlyd
  HVhbElkZW50aXR5IhcKFUNsaWVudFZpcnR1YWxJZGVudGl0eSKNAQoNTGF5ZXJJZGVudGl0eRIpCgh3b3JrZmxvdxgBIAIoCUIN4
  j8KEgh3b3JrZmxvd1IId29ya2Zsb3cSKQoIb3BlcmF0b3IYAiACKAlCDeI/ChIIb3BlcmF0b3JSCG9wZXJhdG9yEiYKB2xheWVyS
  UQYAyACKAlCDOI/CRIHbGF5ZXJJRFIHbGF5ZXJJRCKmAQoMTGlua0lkZW50aXR5Ek0KBGZyb20YASACKAsyLi5lZHUudWNpLmljc
  y5hbWJlci5lbmdpbmUuY29tbW9uLkxheWVySWRlbnRpdHlCCeI/BhIEZnJvbVIEZnJvbRJHCgJ0bxgCIAIoCzIuLmVkdS51Y2kua
  WNzLmFtYmVyLmVuZ2luZS5jb21tb24uTGF5ZXJJZGVudGl0eUIH4j8EEgJ0b1ICdG8iaAoQT3BlcmF0b3JJZGVudGl0eRIpCgh3b
  3JrZmxvdxgBIAIoCUIN4j8KEgh3b3JrZmxvd1IId29ya2Zsb3cSKQoIb3BlcmF0b3IYAiACKAlCDeI/ChIIb3BlcmF0b3JSCG9wZ
  XJhdG9yIisKEFdvcmtmbG93SWRlbnRpdHkSFwoCaWQYASACKAlCB+I/BBICaWRSAmlk"""
        )
        .mkString
    )
  lazy val scalaDescriptor: _root_.scalapb.descriptors.FileDescriptor = {
    val scalaProto = com.google.protobuf.descriptor.FileDescriptorProto.parseFrom(ProtoBytes)
    _root_.scalapb.descriptors.FileDescriptor
      .buildFrom(scalaProto, dependencies.map(_.scalaDescriptor))
  }
  lazy val javaDescriptor: com.google.protobuf.Descriptors.FileDescriptor = {
    val javaProto = com.google.protobuf.DescriptorProtos.FileDescriptorProto.parseFrom(ProtoBytes)
    com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
      javaProto,
      _root_.scala.Array(
      )
    )
  }
  @deprecated(
    "Use javaDescriptor instead. In a future version this will refer to scalaDescriptor.",
    "ScalaPB 0.5.47"
  )
  def descriptor: com.google.protobuf.Descriptors.FileDescriptor = javaDescriptor
}
