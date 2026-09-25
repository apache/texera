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

import com.fasterxml.jackson.core.{JsonParser, JsonStreamContext, JsonToken}
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
import org.apache.texera.amber.core.state.StateReferencing.{
  escapePointerSegment,
  referencedVariable
}

import java.lang.reflect.Modifier
import java.util
import scala.collection.mutable

/**
  * Parses a `$K` loop-variable reference in a typed property of a `StateReferencing` descriptor.
  *
  * The frontend sends the literal string `"$K"` whatever the property's type, so an Int property
  * such as Limit's `limit` would not parse. Jackson asks its problem handlers what to do with a
  * string it cannot convert. While a `StateReferencing` object is parsed, this module's handler
  * answers a whole-string `$name` meant for an integral, floating-point or boolean value with the
  * placeholder `0` / `0.0` / `false`, and records the value's JSON pointer, relative to the
  * object, -> `name`; the object's `stateReferences` sidecar is set to what was recorded. Any
  * other target (an enum, a date, ...) keeps Jackson's ordinary error. A String property simply
  * keeps the literal: the compiler finds those itself, and only inside a loop block
  * (`WorkflowCompiler.normalizeStateReferences`).
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

  /** A `StateReferencing` object being parsed: its parser, its own parse context, its references. */
  private final class Frame(val parser: JsonParser, val context: JsonStreamContext) {
    val references: mutable.Map[String, String] = mutable.Map.empty
  }

  /** The key of the frames open in one deserialization (innermost first), kept on its context. */
  private object Frames

  private def frames(ctxt: DeserializationContext): util.Deque[Frame] =
    ctxt.getAttribute(Frames) match {
      case open: util.Deque[Frame @unchecked] => open
      case _ =>
        val open = new util.ArrayDeque[Frame]()
        ctxt.setAttribute(Frames, open)
        open
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
        frame <- Option(frames(ctxt).peek())
        // The object's parser must stand on this very string: one that a nested object Jackson
        // replays from a buffer of its own does not, and its pointer cannot be told from here.
        if frame.parser.hasToken(JsonToken.VALUE_STRING) && frame.parser.getText == valueToConvert
        pointer <- pointerOf(frame)
      } yield {
        frame.references(pointer) = name
        placeholder
      }
      placeholder.getOrElse(DeserializationProblemHandler.NOT_HANDLED)
    }
  }

  /** The JSON pointer of the value the frame's parser stands on, relative to the frame's object. */
  private def pointerOf(frame: Frame): Option[String] = {
    def segment(context: JsonStreamContext): String =
      "/" + (if (context.inArray) context.getCurrentIndex.toString
             else escapePointerSegment(context.getCurrentName))
    var context = frame.parser.getParsingContext
    var pointer = ""
    while (context != null && (context ne frame.context)) {
      pointer = segment(context) + pointer
      context = context.getParent
    }
    Option.when(context != null)(segment(context) + pointer)
  }

  /**
    * Wraps the deserializer of one concrete `StateReferencing` class in a recording frame. A
    * polymorphic type deserializer hands the object over mid-way, once it has read the type id;
    * when the id was not the first field, it replays the fields before it from a buffer whose
    * parse context is a copy of the first field's VALUE's context, not of the object's. So an
    * object handed over mid-way is first copied into a buffer of its own, which starts at the
    * object's START_OBJECT; the frame always opens there and every pointer is relative to it.
    */
  private final class RecordingDeserializer(delegate: JsonDeserializer[_])
      extends DelegatingDeserializer(delegate) {

    override protected def newDelegatingInstance(
        newDelegatee: JsonDeserializer[_]
    ): JsonDeserializer[_] = new RecordingDeserializer(newDelegatee)

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): AnyRef =
      p.currentToken() match {
        case JsonToken.START_OBJECT => recorded(p, ctxt)
        case JsonToken.FIELD_NAME   => recorded(restOfObject(p, ctxt), ctxt)
        case _                      => _delegatee.deserialize(p, ctxt).asInstanceOf[AnyRef]
      }

    private def recorded(parser: JsonParser, ctxt: DeserializationContext): AnyRef = {
      val frame = new Frame(parser, parser.getParsingContext)
      val open = frames(ctxt)
      open.push(frame)
      val bean =
        try _delegatee.deserialize(parser, ctxt).asInstanceOf[AnyRef]
        finally open.pop()
      bean.asInstanceOf[StateReferencing].stateReferences = frame.references.toMap
      bean
    }
  }

  /** The fields left at `p` as a separate object, parsed from its START_OBJECT on; `p` ends on END_OBJECT. */
  private def restOfObject(p: JsonParser, ctxt: DeserializationContext): JsonParser = {
    val buffer = ctxt.bufferForInputBuffering(p).overrideParentContext(null)
    buffer.writeStartObject()
    while (p.hasToken(JsonToken.FIELD_NAME)) {
      buffer.copyCurrentStructure(p)
      p.nextToken()
    }
    buffer.writeEndObject()
    val parser = buffer.asParser(p)
    parser.nextToken()
    parser
  }
}
