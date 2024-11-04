package edu.uci.ics.amber.operator.common

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.noctordeser.NoCtorDeserModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.{Files, Path, Paths}
import java.text.SimpleDateFormat
import scala.annotation.tailrec

object Utils extends LazyLogging {
  /** An unmodifiable set containing some common URL words that are not usually useful
    * for searching.
    */
  final val URL_STOP_WORDS_SET = List[String](
    "http",
    "https",
    "org",
    "net",
    "com",
    "store",
    "www",
    "html"
  )
}
