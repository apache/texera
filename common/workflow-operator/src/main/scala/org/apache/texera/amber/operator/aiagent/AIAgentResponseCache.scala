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
 */

package org.apache.texera.amber.operator.aiagent

import java.security.MessageDigest

class AIAgentResponseCache(capacity: Int = 1000) extends Serializable {
  private val map: java.util.LinkedHashMap[String, String] =
    new java.util.LinkedHashMap[String, String](capacity, 0.75f, true) {
      override def removeEldestEntry(eldest: java.util.Map.Entry[String, String]): Boolean =
        this.size() > capacity
    }

  def get(key: String): Option[String] = map.synchronized {
    Option(map.get(key))
  }

  def put(key: String, value: String): Unit = map.synchronized {
    map.put(key, value)
  }

  def size: Int = map.synchronized(map.size())
}

object AIAgentResponseCache {
  def key(
      model: String,
      temperature: Double,
      apiKeySignature: String,
      systemPrompt: String,
      userPrompt: String,
      toolSig: String,
      structSig: String
  ): String = {
    val canonical =
      s"${Option(model).getOrElse("")}|$temperature|$apiKeySignature|${Option(systemPrompt).getOrElse("")}|" +
        s"${Option(userPrompt).getOrElse("")}|$toolSig|$structSig"
    sha256(canonical)
  }

  def sha256(text: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(Option(text).getOrElse("").getBytes("UTF-8"))
    md.digest().map("%02x".format(_)).mkString
  }
}
