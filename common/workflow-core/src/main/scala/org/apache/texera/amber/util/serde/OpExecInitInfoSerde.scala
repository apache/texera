/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.util.serde

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonNode,
  JsonSerializer,
  SerializerProvider
}
import org.apache.texera.amber.core.executor.{
  OpExecInitInfo,
  OpExecSource,
  OpExecWithClassName,
  OpExecWithCode
}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity

/**
  * JSON field names used by [[OpExecInitInfoSerializer]] / [[OpExecInitInfoDeserializer]].
  * Centralized so the two halves cannot drift apart.
  */
private object OpExecInitInfoSerde {
  val KindField = "kind"
  val ClassNameKind = "className"
  val CodeKind = "code"
  val SourceKind = "source"
  val EmptyKind = "empty"

  val ClassNameField = "className"
  val DescStringField = "descString"
  val CodeField = "code"
  val LanguageField = "language"
  val StorageKeyField = "storageKey"
  val WorkflowIdentityField = "workflowIdentity"
}

/**
  * Serializes the scalapb sealed-oneof [[OpExecInitInfo]] to a tagged JSON object.
  *
  * `OpExecInitInfo` is a scalapb `GeneratedSealedOneof`; Jackson cannot introspect its
  * three concrete subtypes (`OpExecWithClassName`, `OpExecWithCode`, `OpExecSource`)
  * via `@JsonTypeInfo`. This serializer maps each subtype to an explicit
  * `{"kind": ...}` discriminator plus its scalar fields. `OpExecSource.workflowIdentity`
  * is a nested scalapb message and is delegated to the surrounding codec.
  */
class OpExecInitInfoSerializer extends JsonSerializer[OpExecInitInfo] {
  override def serialize(
      value: OpExecInitInfo,
      gen: JsonGenerator,
      serializers: SerializerProvider
  ): Unit = {
    gen.writeStartObject()
    value match {
      case OpExecWithClassName(className, descString) =>
        gen.writeStringField(OpExecInitInfoSerde.KindField, OpExecInitInfoSerde.ClassNameKind)
        gen.writeStringField(OpExecInitInfoSerde.ClassNameField, className)
        gen.writeStringField(OpExecInitInfoSerde.DescStringField, descString)
      case OpExecWithCode(code, language) =>
        gen.writeStringField(OpExecInitInfoSerde.KindField, OpExecInitInfoSerde.CodeKind)
        gen.writeStringField(OpExecInitInfoSerde.CodeField, code)
        gen.writeStringField(OpExecInitInfoSerde.LanguageField, language)
      case OpExecSource(storageKey, workflowIdentity) =>
        gen.writeStringField(OpExecInitInfoSerde.KindField, OpExecInitInfoSerde.SourceKind)
        gen.writeStringField(OpExecInitInfoSerde.StorageKeyField, storageKey)
        gen.writeObjectField(OpExecInitInfoSerde.WorkflowIdentityField, workflowIdentity)
      case OpExecInitInfo.Empty =>
        gen.writeStringField(OpExecInitInfoSerde.KindField, OpExecInitInfoSerde.EmptyKind)
    }
    gen.writeEndObject()
  }
}

/**
  * Reconstructs an [[OpExecInitInfo]] from the tagged JSON object produced by
  * [[OpExecInitInfoSerializer]]. The resulting value is the same concrete subtype the
  * engine pattern-matches on in `SerializationManager` / `ExecFactory`, so the
  * round-tripped operator can still build a runnable executor.
  */
class OpExecInitInfoDeserializer extends JsonDeserializer[OpExecInitInfo] {
  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): OpExecInitInfo = {
    val codec = p.getCodec
    val node: JsonNode = codec.readTree(p)
    val kindNode = node.get(OpExecInitInfoSerde.KindField)
    if (kindNode == null) {
      throw new IllegalArgumentException(
        s"Cannot deserialize OpExecInitInfo: missing '${OpExecInitInfoSerde.KindField}' field in $node"
      )
    }

    def text(field: String): String = {
      val n = node.get(field)
      if (n == null || n.isNull) "" else n.asText()
    }

    kindNode.asText() match {
      case OpExecInitInfoSerde.ClassNameKind =>
        OpExecWithClassName(
          text(OpExecInitInfoSerde.ClassNameField),
          text(OpExecInitInfoSerde.DescStringField)
        )
      case OpExecInitInfoSerde.CodeKind =>
        OpExecWithCode(
          text(OpExecInitInfoSerde.CodeField),
          text(OpExecInitInfoSerde.LanguageField)
        )
      case OpExecInitInfoSerde.SourceKind =>
        val wfNode = node.get(OpExecInitInfoSerde.WorkflowIdentityField)
        val workflowIdentity =
          if (wfNode == null || wfNode.isNull) WorkflowIdentity.defaultInstance
          else codec.treeToValue(wfNode, classOf[WorkflowIdentity])
        OpExecSource(text(OpExecInitInfoSerde.StorageKeyField), workflowIdentity)
      case OpExecInitInfoSerde.EmptyKind =>
        OpExecInitInfo.Empty
      case other =>
        throw new IllegalArgumentException(s"Unknown OpExecInitInfo kind: $other")
    }
  }
}
