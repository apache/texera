package edu.ics.uci.amber.model

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.noctordeser.NoCtorDeserModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.text.SimpleDateFormat

object Utils  {

  final val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new NoCtorDeserModule())
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_ABSENT)
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

}
