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

package org.apache.texera.service.`type`.serde

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.texera.service.`type`.DatasetFileNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetFileNodeSerializerSpec extends AnyFlatSpec with Matchers {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    // DefaultScalaModule lets Jackson unwrap scala.Option for the "size" field.
    m.registerModule(DefaultScalaModule)
    val module = new SimpleModule()
    module.addSerializer(classOf[DatasetFileNode], new DatasetFileNodeSerializer())
    m.registerModule(module)
    m
  }

  private def asJson(node: DatasetFileNode): JsonNode =
    mapper.readTree(mapper.writeValueAsString(node))

  // The serializer dereferences value.getParent().getFilePath(), so every node it
  // sees needs a non-null parent. Tests build a tree rooted at "/" and serialize
  // its descendants.
  private def rootDir: DatasetFileNode =
    new DatasetFileNode("/", "directory", null, "")

  "DatasetFileNodeSerializer" should "serialize a file node with size and no children field" in {
    val root = rootDir
    val owner = new DatasetFileNode("alice@example.com", "directory", root, "alice@example.com")
    val file = new DatasetFileNode("data.csv", "file", owner, "alice@example.com", Some(100L))

    val json = asJson(file)

    json.get("name").asText() shouldBe "data.csv"
    json.get("type").asText() shouldBe "file"
    json.get("parentDir").asText() shouldBe "/alice@example.com"
    json.get("ownerEmail").asText() shouldBe "alice@example.com"
    json.get("size").asLong() shouldBe 100L
    json.has("children") shouldBe false
  }

  it should "recursively serialize a directory and its children" in {
    val root = rootDir
    val owner = new DatasetFileNode("alice@example.com", "directory", root, "alice@example.com")
    val file = new DatasetFileNode("data.csv", "file", owner, "alice@example.com", Some(100L))
    val subdir = new DatasetFileNode("subdir", "directory", owner, "alice@example.com")
    val nested = new DatasetFileNode("nested.txt", "file", subdir, "alice@example.com", Some(200L))
    subdir.children = Some(List(nested))
    owner.children = Some(List(file, subdir))

    val json = asJson(owner)

    json.get("name").asText() shouldBe "alice@example.com"
    json.get("type").asText() shouldBe "directory"
    json.get("parentDir").asText() shouldBe "/"
    val children = json.get("children")
    children.isArray shouldBe true
    children.size() shouldBe 2
    children.get(0).get("name").asText() shouldBe "data.csv"
    children.get(0).get("size").asLong() shouldBe 100L
    children.get(1).get("name").asText() shouldBe "subdir"
    children.get(1).get("children").get(0).get("name").asText() shouldBe "nested.txt"
    children.get(1).get("children").get(0).get("size").asLong() shouldBe 200L
  }

  it should "emit an empty children array for a directory with no children" in {
    val root = rootDir
    val empty = new DatasetFileNode("empty", "directory", root, "alice@example.com")

    val json = asJson(empty)

    json.get("type").asText() shouldBe "directory"
    val children = json.get("children")
    children.isArray shouldBe true
    children.size() shouldBe 0
  }
}
