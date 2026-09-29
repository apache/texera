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

package org.apache.texera.amber.core.state

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer
import com.fasterxml.jackson.databind.deser.{
  BeanDeserializerModifier,
  DeserializationProblemHandler
}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{
  BeanDescription,
  DeserializationConfig,
  DeserializationContext,
  JsonDeserializer,
  Module
}
import org.apache.texera.amber.core.state.StateReferencing.referencedVariable

import java.lang.reflect.Modifier
import scala.collection.mutable

/**
  * Parses a `$K` loop-variable reference in a typed property of a `StateReferencing` descriptor.
  *
  * The frontend sends the literal string `"$K"` whatever the property's type, so an Int property
  * such as Limit's `limit` would not parse. When Jackson cannot convert a whole-string `$name` to
  * an integral, floating-point or boolean value, this module's handler puts the placeholder `0` /
  * `0.0` / `false` there and records the value's JSON pointer, relative to the object, -> `name`
  * in the object's `stateReferences` sidecar. Any other target keeps Jackson's ordinary error. A
  * String property keeps the literal: the compiler finds those itself, and only inside a loop
  * block (`WorkflowCompiler.normalizeStateReferences`).
  */
class StateReferenceModule extends SimpleModule("StateReferenceModule") {

  setDeserializerModifier(new BeanDeserializerModifier {
    override def modifyDeserializer(
        config: DeserializationConfig,
        beanDesc: BeanDescription,
        deserializer: JsonDeserializer[_]
    ): JsonDeserializer[_] = {
      val beanClass = beanDesc.getBeanClass
      if (
        classOf[StateReferencing].isAssignableFrom(beanClass) &&
        !Modifier.isAbstract(beanClass.getModifiers)
      ) {
        new StateReferenceModule.RecordingDeserializer(deserializer)
      } else {
        deserializer
      }
    }
  })

  override def setupModule(context: Module.SetupContext): Unit = {
    super.setupModule(context)
    context.addDeserializationProblemHandler(StateReferenceModule.PlaceholderHandler)
  }
}

object StateReferenceModule {

  /** The placeholder for each target Jackson converts a string to: primitive and boxed. */
  private val Placeholders: Map[Class[_], AnyRef] =
    Seq[(Class[_], Class[_], AnyRef)](
      (classOf[Int], classOf[java.lang.Integer], Int.box(0)),
      (classOf[Long], classOf[java.lang.Long], Long.box(0L)),
      (classOf[Short], classOf[java.lang.Short], Short.box(0)),
      (classOf[Double], classOf[java.lang.Double], Double.box(0.0)),
      (classOf[Float], classOf[java.lang.Float], Float.box(0f)),
      (classOf[Boolean], classOf[java.lang.Boolean], Boolean.box(false))
    ).flatMap { case (primitive, boxed, zero) => Seq(primitive -> zero, boxed -> zero) }.toMap

  /**
    * The innermost `StateReferencing` object being parsed, kept on the context under
    * `classOf[Frame]`: the parser over its own copy, and the references recorded so far.
    */
  private final class Frame(val parser: JsonParser) {
    val references: mutable.Map[String, String] = mutable.Map.empty
  }

  private object PlaceholderHandler extends DeserializationProblemHandler {
    override def handleWeirdStringValue(
        ctxt: DeserializationContext,
        targetType: Class[_],
        valueToConvert: String,
        failureMsg: String
    ): AnyRef = {
      val placeholder = for {
        name <- referencedVariable(valueToConvert)
        placeholder <- Placeholders.get(targetType)
        frame <- Option(ctxt.getAttribute(classOf[Frame]).asInstanceOf[Frame])
        // The object's parser must stand on this very string: one that a nested object Jackson
        // replays from a buffer of its own does not, and its pointer cannot be told from here.
        if frame.parser.hasToken(JsonToken.VALUE_STRING) && frame.parser.getText == valueToConvert
      } yield {
        // The parser reads the object's own copy, so its path is relative to the object.
        frame.references(frame.parser.getParsingContext.pathAsPointer.toString) = name
        placeholder
      }
      placeholder.getOrElse(DeserializationProblemHandler.NOT_HANDLED)
    }
  }

  /**
    * Wraps the deserializer of one concrete `StateReferencing` class, and parses each object from
    * a copy of its own: a polymorphic type deserializer hands the object over mid-way, after the
    * type id, and may replay the fields before it from a buffer whose parse context is not the
    * object's.
    */
  private final class RecordingDeserializer(delegate: JsonDeserializer[_])
      extends DelegatingDeserializer(delegate) {

    override protected def newDelegatingInstance(
        newDelegatee: JsonDeserializer[_]
    ): JsonDeserializer[_] = new RecordingDeserializer(newDelegatee)

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): AnyRef =
      if (p.hasToken(JsonToken.START_OBJECT) || p.hasToken(JsonToken.FIELD_NAME)) {
        recorded(ownCopy(p, ctxt), ctxt)
      } else {
        _delegatee.deserialize(p, ctxt).asInstanceOf[AnyRef]
      }

    private def recorded(parser: JsonParser, ctxt: DeserializationContext): AnyRef = {
      val frame = new Frame(parser)
      val enclosing = ctxt.getAttribute(classOf[Frame])
      ctxt.setAttribute(classOf[Frame], frame)
      val bean =
        try _delegatee.deserialize(parser, ctxt).asInstanceOf[AnyRef]
        finally ctxt.setAttribute(classOf[Frame], enclosing)
      bean.asInstanceOf[StateReferencing].stateReferences = frame.references.toMap
      bean
    }
  }

  /** The object `p` stands in, as a parser on the START_OBJECT of a copy; `p` ends on its end. */
  private def ownCopy(p: JsonParser, ctxt: DeserializationContext): JsonParser = {
    val buffer = ctxt.bufferForInputBuffering(p).overrideParentContext(null)
    if (p.hasToken(JsonToken.START_OBJECT)) {
      buffer.copyCurrentStructure(p)
    } else {
      buffer.writeStartObject()
      while (p.hasToken(JsonToken.FIELD_NAME)) {
        buffer.copyCurrentStructure(p)
        p.nextToken()
      }
      buffer.writeEndObject()
    }
    val parser = buffer.asParser(p)
    parser.nextToken()
    parser
  }
}
