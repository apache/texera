package edu.uci.ics.texera.web.util

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.Utils

import java.io.OutputStream
import scala.collection.mutable

/** Eagerly primes Jackson’s caches so that the first live websocket
 * serialisation/deserialisation does **zero** reflective work.
 */
object JacksonPrewarm extends LazyLogging {

  private val mapper: ObjectMapper = {
    // make sure Afterburner is present *before* we start warming
    val m = Utils.objectMapper
    if (!m.getRegisteredModuleIds.contains(classOf[AfterburnerModule].getName)) {
      m.registerModule(new AfterburnerModule())
    }
    m
  }

  /** Call this once during Dropwizard bootstrap. */
  def prewarm(): Unit = {
    val t0 = System.currentTimeMillis()

    val roots = Seq(
      classOf[edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest],
      classOf[edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent],
      classOf[edu.uci.ics.amber.operator.LogicalOp]
    )

    val all = roots.flatMap(collectRecursively).toSet
    all.foreach { cls =>
      try {
        mapper.registerSubtypes(cls)                       // polymorphic scan shortcut
        val jt = mapper.getTypeFactory.constructType(cls)
        mapper.getDeserializationContext
          .findRootValueDeserializer(jt)               // build & cache deserializer
      } catch {
        case th: Throwable =>
          logger.debug(s"Pre-warm skipped ${cls.getName}: ${th.getMessage}")
      }
    }

    logger.info(s"Jackson pre-warm cached ${all.size} classes in ${System.currentTimeMillis - t0} ms")
  }
  /* ----------------------------------------------------------------------- */

  /** Breadth-first traversal of @JsonSubTypes annotations. */
  private def collectRecursively(root: Class[_]): Set[Class[_]] = {
    val visited  = mutable.Set[Class[_]]()
    val worklist = mutable.Queue[Class[_]](root)

    while (worklist.nonEmpty) {
      val c = worklist.dequeue()
      if (visited.add(c)) {
        Option(c.getAnnotation(classOf[JsonSubTypes])).foreach { ann =>
          ann.value().foreach { sub =>
            if (!visited.contains(sub.value())) worklist += sub.value()
          }
        }
      }
    }
    visited.toSet
  }

  /** Registers the class and forces Jackson to materialise its codec. */
  private def buildSerialisers(cls: Class[_]): Unit = {
    try {
      mapper.registerSubtypes(cls)

      val jt: JavaType = mapper.getTypeFactory.constructType(cls)

      // serializer
      val ser: JsonSerializer[AnyRef] =
        mapper.getSerializerProvider.findTypedValueSerializer(jt, true, null)
          .asInstanceOf[JsonSerializer[AnyRef]]

      // deserializer
      mapper.getDeserializationContext.findRootValueDeserializer(jt)

      // optional: dummy round-trip to finalise Afterburner byte-code
      instantiateAndSerialize(ser, cls)

    } catch {
      case th: Throwable =>
        logger.debug(s"Pre-warm skipped ${cls.getName}: ${th.getMessage}")
    }
  }

  /** Tries to create a dummy object (when possible) and serialise it
   * to `/dev/null` to trigger Afterburner’s fast-path once at startup.
   */
  private def instantiateAndSerialize(ser: JsonSerializer[AnyRef], cls: Class[_]): Unit = {
    try {
      val ctor   = cls.getDeclaredConstructor()
      ctor.setAccessible(true)
      val dummy  = ctor.newInstance().asInstanceOf[AnyRef]
      val gen: JsonGenerator =
        mapper.getFactory.createGenerator(OutputStream.nullOutputStream())

      ser.serialize(dummy, gen, mapper.getSerializerProvider)
      gen.close()
    } catch {
      case _: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException =>
        () // not instantiable – silently ignore
    }
  }
}
