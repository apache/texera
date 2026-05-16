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

package org.apache.texera.web.resource

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.operator.source.scan.smart.{
  InferenceOverrides,
  SmartFileFormat,
  SmartFileInferencer
}

import javax.annotation.security.RolesAllowed
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, Produces}
import scala.jdk.CollectionConverters._

@JsonIgnoreProperties(ignoreUnknown = true)
case class SmartFileInferenceRequest(
    @JsonProperty("fileName") fileName: String,
    @JsonProperty("fileEncoding") fileEncoding: Option[String] = None,
    @JsonProperty("formatOverride") formatOverride: Option[String] = None,
    @JsonProperty("customDelimiter") customDelimiter: Option[String] = None,
    @JsonProperty("hasHeader") hasHeader: Option[Boolean] = None,
    @JsonProperty("sheetName") sheetName: Option[String] = None,
    @JsonProperty("flatten") flatten: Option[Boolean] = None
)

case class SmartFileInferenceColumn(name: String, `type`: String)

case class SmartFileInferenceResponse(
    detectedFormat: String,
    schema: java.util.List[SmartFileInferenceColumn],
    customDelimiter: String,
    hasHeader: java.lang.Boolean,
    sheetName: String,
    availableSheetNames: java.util.List[String],
    flatten: java.lang.Boolean,
    isFolder: Boolean,
    fileCount: Int
)

@Path("/file-inference")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SmartFileInferenceResource {

  @POST
  @Path("/preview")
  def preview(request: SmartFileInferenceRequest): SmartFileInferenceResponse = {
    val uri = FileResolver.resolve(request.fileName)
    val charset = request.fileEncoding
      .flatMap(name => tryParseEncoding(name))
      .getOrElse(FileDecodingMethod.UTF_8.getCharset)

    val overrides = InferenceOverrides(
      format = request.formatOverride.flatMap(s => tryParseFormat(s)),
      delimiter = request.customDelimiter.flatMap(_.headOption),
      hasHeader = request.hasHeader,
      sheetName = request.sheetName,
      flatten = request.flatten
    )

    val result = SmartFileInferencer.infer(uri, charset, overrides)
    val columns = result.schema.getAttributes
      .map(a => SmartFileInferenceColumn(a.getName, a.getType.toString))
      .asJava

    SmartFileInferenceResponse(
      detectedFormat = result.format.getLabel,
      schema = columns,
      customDelimiter = result.csvDelimiter.orNull,
      hasHeader = result.csvHasHeader.map(java.lang.Boolean.valueOf).orNull,
      sheetName = result.sheetName.orNull,
      availableSheetNames = result.availableSheetNames.asJava,
      flatten = result.flatten.map(java.lang.Boolean.valueOf).orNull,
      isFolder = result.isFolder,
      fileCount = result.fileCount
    )
  }

  private def tryParseFormat(value: String): Option[SmartFileFormat] = {
    val upper = value.toUpperCase
    // Accept both the enum name (CSV, TSV, ...) and the user-facing label ("Plain text", ...).
    try Some(SmartFileFormat.valueOf(upper))
    catch {
      case _: IllegalArgumentException =>
        SmartFileFormat.values().find(_.getLabel.equalsIgnoreCase(value))
    }
  }

  private def tryParseEncoding(value: String): Option[java.nio.charset.Charset] =
    try Some(FileDecodingMethod.valueOf(value.toUpperCase).getCharset)
    catch { case _: IllegalArgumentException => None }
}
