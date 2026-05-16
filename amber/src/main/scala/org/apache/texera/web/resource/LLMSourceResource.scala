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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import kong.unirest.Unirest
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver}
import org.apache.texera.amber.config.PythonUtils
import org.apache.texera.amber.operator.source.scan.FolderInputResolver
import org.apache.texera.config.LLMConfig
import play.api.libs.json.{JsObject, Json}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import javax.annotation.security.RolesAllowed
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path => WsPath, Produces, WebApplicationException}
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.Try

@JsonIgnoreProperties(ignoreUnknown = true)
case class LLMSourceGenerateRequest(
    @JsonProperty("fileName") fileName: String,
    @JsonProperty("userHint") userHint: Option[String] = None,
    @JsonProperty("llmModel") llmModel: Option[String] = None,
    // Iterative repair contract — accepted from day one even though MVP UI doesn't use it.
    @JsonProperty("previousCode") previousCode: Option[String] = None,
    @JsonProperty("previousError") previousError: Option[String] = None
)

case class LLMSourceColumn(name: String, `type`: String)

case class LLMSourceTable(
    name: String,
    description: String,
    columns: java.util.List[LLMSourceColumn]
)

case class LLMSourceGenerateResponse(
    generatedCode: String,
    tables: java.util.List[LLMSourceTable],
    unionColumns: java.util.List[LLMSourceColumn],
    llmModel: String,
    sampleHash: String,
    generatedAt: String,
    warnings: java.util.List[String]
)

@WsPath("/llm-source")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class LLMSourceResource extends LazyLogging {

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  // Must match a model name registered in the LiteLLM proxy's litellm-config.yaml.
  // See bin/litellm-config.yaml for the registered list.
  private val DefaultModel = "claude-haiku-4.5"
  private val SampleByteCount = 64 * 1024
  private val BinarySampleByteCount = 2 * 1024
  // 2000 tokens covers a parser + a handful of table schemas. We measured Claude Haiku 4.5 at
  // ~50 tokens/sec end-to-end through the LiteLLM proxy, so this caps the LLM call at ~40s.
  private val MaxTokens = 2000
  private val ConnectTimeoutMs = 10_000
  private val SocketTimeoutMs = 90_000

  // After getting code from the LLM, dry-run it against the real file. If it raises, feed the
  // traceback back into the LLM and ask for a fix. Two retries (so up to three LLM calls
  // total) keeps the worst-case latency reasonable while letting us self-heal most flaky outputs.
  private val MaxDryRunRetries = 2
  private val DryRunSampleRows = 5

  // Probing for a working python binary spawns 1 process per candidate; cache the result so we
  // pay that cost at most once per backend lifetime instead of on every Generate request.
  @volatile private var cachedPythonBinary: Option[Option[String]] = None

  @POST
  @WsPath("/generate")
  def generate(request: LLMSourceGenerateRequest): LLMSourceGenerateResponse = {
    if (request.fileName == null || request.fileName.trim.isEmpty) {
      throw new WebApplicationException("fileName is required", 400)
    }

    val tOverall = System.currentTimeMillis()
    val uri = FileResolver.resolve(request.fileName)
    val resolved = FolderInputResolver.resolve(uri)
    val sampleSource = resolved.files.headOption
      .map(_.uri)
      .getOrElse(uri)
    val displayName = resolved.files.headOption.map(_.displayName).getOrElse(request.fileName)
    val extension = extractExtension(displayName)

    val tSample = System.currentTimeMillis()
    val (samplePreview, sampleBytes) = buildSample(sampleSource, extension)
    val sampleHash = sha256Hex(sampleBytes)
    val sampleMs = System.currentTimeMillis() - tSample

    // Resolve once here so the dry-run sees the same local path the workflow will use at run time.
    // For folder inputs, this must stay a folder path rather than degrading to the sampled first file.
    val resolvedLocalFilePath = Try(resolveRuntimeInputPath(uri).toAbsolutePath.toString)
      .getOrElse(request.fileName)

    val model = request.llmModel.filter(_.nonEmpty).getOrElse(DefaultModel)
    val warnings = new java.util.ArrayList[String]()

    // ---- LLM call with dry-run retry loop ---------------------------------
    var attempt = 0
    var lastCode: String = ""
    var lastTables: List[ParsedTable] = Nil
    var lastRawResponse: String = ""
    var lastSyntaxError: String = ""
    var lastDryRunError: String = ""
    var lastDryRunRowsSeen: Int = 0
    var llmMsTotal: Long = 0
    var validateMsTotal: Long = 0
    var success = false

    while (!success && attempt <= MaxDryRunRetries) {
      val previousCode = if (attempt == 0) request.previousCode.getOrElse("") else lastCode
      val previousError = if (attempt == 0) request.previousError.getOrElse("")
      else if (lastDryRunError.nonEmpty) lastDryRunError
      else lastSyntaxError

      val prompt = buildPrompt(
        displayName = displayName,
        extension = extension,
        isFolder = resolved.isFolder,
        fileCount = resolved.files.size,
        userHint = request.userHint.getOrElse(""),
        samplePreview = samplePreview,
        previousCode = previousCode,
        previousError = previousError
      )

      val tLlm = System.currentTimeMillis()
      val rawLlmContent = callLLM(model, prompt)
      llmMsTotal += System.currentTimeMillis() - tLlm
      lastRawResponse = rawLlmContent

      val parsed = parseLLMResponse(rawLlmContent)
      lastCode = parsed.code
      lastTables = parsed.tables

      val tValidate = System.currentTimeMillis()
      val syntaxResult = validatePython(lastCode)
      lastSyntaxError = syntaxResult.getOrElse("")
      validateMsTotal += System.currentTimeMillis() - tValidate

      if (syntaxResult.isDefined) {
        logger.warn(s"generate attempt $attempt failed syntax check: ${syntaxResult.get.take(400)}")
        attempt += 1
      } else {
        val dryRun = dryRunGeneratedCode(lastCode, resolvedLocalFilePath)
        if (dryRun.ok) {
          lastDryRunError = ""
          lastDryRunRowsSeen = dryRun.rowsSeen
          success = true
          logger.info(s"generate attempt $attempt dry-run OK: rowsSeen=${dryRun.rowsSeen}")
        } else {
          lastDryRunError = dryRun.error.getOrElse("dry-run failed with no error message")
          logger.warn(s"generate attempt $attempt dry-run failed: ${lastDryRunError.take(400)}")
          attempt += 1
        }
      }
    }

    if (!success) {
      if (lastSyntaxError.nonEmpty) warnings.add(s"Generated code did not parse: $lastSyntaxError")
      if (lastDryRunError.nonEmpty) warnings.add(s"Generated code failed dry-run: $lastDryRunError")
      warnings.add(
        s"Exhausted ${MaxDryRunRetries + 1} attempts. Returning the last code anyway so you can hand-edit it."
      )
    }

    val totalMs = System.currentTimeMillis() - tOverall
    logger.info(
      s"generate: total=${totalMs}ms (sample=${sampleMs}ms, llm=${llmMsTotal}ms across ${attempt + (if (success) 1 else 0)} attempts, " +
        s"validate=${validateMsTotal}ms, model=$model, prompt_chars_last=${lastRawResponse.length}, " +
        s"success=$success, rowsSeen=$lastDryRunRowsSeen)"
    )

    val tableList = new java.util.ArrayList[LLMSourceTable]()
    val seenColumns = scala.collection.mutable.LinkedHashMap[String, String]()
    seenColumns.put("__table__", "string")
    lastTables.foreach { t =>
      val cols = new java.util.ArrayList[LLMSourceColumn]()
      t.columns.foreach { c =>
        cols.add(LLMSourceColumn(c.name, c.`type`))
        if (!seenColumns.contains(c.name)) {
          seenColumns.put(c.name, c.`type`)
        } else if (seenColumns(c.name) != c.`type`) {
          warnings.add(
            s"Column '${c.name}' appears with different types across tables (${seenColumns(c.name)} vs ${c.`type`}); using ${seenColumns(c.name)} in the union schema."
          )
        }
      }
      tableList.add(LLMSourceTable(t.name, t.description, cols))
    }
    val union = seenColumns.toList.map { case (n, t) => LLMSourceColumn(n, t) }.asJava

    LLMSourceGenerateResponse(
      generatedCode = lastCode,
      tables = tableList,
      unionColumns = union,
      llmModel = model,
      sampleHash = sampleHash,
      generatedAt = Instant.now().toString,
      warnings = warnings
    )
  }

  // ---------------------------------------------------------------------------
  // Dry-run of the generated UDFSourceOperator against the real file.
  // ---------------------------------------------------------------------------

  private case class DryRunResult(ok: Boolean, rowsSeen: Int, samples: Seq[String], error: Option[String])

  private[resource] def resolveRuntimeInputPath(uri: URI): Path =
    FolderInputResolver.materializeToLocalPath(uri)

  private val pythonSrcPath: String = {
    val candidates = Seq(
      "amber/src/main/python",
      "../amber/src/main/python",
      "../../amber/src/main/python"
    )
    candidates.find(p => Files.exists(java.nio.file.Paths.get(p))).getOrElse(candidates.head)
  }

  private val dryRunHarnessPath: String = {
    val candidates = Seq(
      "amber/src/main/scala/org/apache/texera/web/resource/llm_source_dry_run.py",
      "../amber/src/main/scala/org/apache/texera/web/resource/llm_source_dry_run.py"
    )
    candidates.find(p => Files.exists(java.nio.file.Paths.get(p))).getOrElse(candidates.head)
  }

  /**
   * Substitute the file URI token into the code, write it to a temp file, and invoke the dry-run
   * harness which exec()s the code, instantiates GenerateOperator(), and iterates produce() for
   * a few rows. The harness reports OK + sample rows or a Python traceback as JSON.
   */
  private def dryRunGeneratedCode(code: String, resolvedFileUri: String): DryRunResult = {
    val pythonBinary = findPythonBinary().getOrElse {
      return DryRunResult(ok = true, rowsSeen = 0, samples = Seq.empty, error = None)
    }
    val substituted = code.replace("__TEXERA_FILE_URI__", resolvedFileUri)
    val tmpFile = Files.createTempFile("texera-llm-source-dryrun-", ".py")
    try {
      Files.write(tmpFile, substituted.getBytes(StandardCharsets.UTF_8))
      val cmd = Seq(
        pythonBinary,
        dryRunHarnessPath,
        tmpFile.toAbsolutePath.toString,
        pythonSrcPath,
        DryRunSampleRows.toString
      )
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val procLogger = ProcessLogger(
        line => { stdout.append(line); stdout.append('\n') },
        line => { stderr.append(line); stderr.append('\n') }
      )
      val exit = Try(cmd.!(procLogger)).getOrElse(-1)
      val rawOut = stdout.toString.trim
      if (exit != 0 && rawOut.isEmpty) {
        return DryRunResult(
          ok = false,
          rowsSeen = 0,
          samples = Seq.empty,
          error = Some(s"dry-run subprocess exited $exit. stderr: ${stderr.toString.trim.take(800)}")
        )
      }
      // The harness prints a single JSON object on stdout.
      val lastJsonLine = rawOut
        .split('\n')
        .reverseIterator
        .find(l => l.startsWith("{"))
        .getOrElse(rawOut)
      try {
        val node = Json.parse(lastJsonLine)
        val ok = (node \ "ok").asOpt[Boolean].getOrElse(false)
        val rowsSeen = (node \ "rowsSeen").asOpt[Int].getOrElse(0)
        val errMsg = (node \ "error").asOpt[String]
        val tb = (node \ "traceback").asOpt[String]
        val samples = (node \ "samples").asOpt[Seq[JsObject]].map(_.map(_.toString())).getOrElse(Seq.empty)
        val combinedError = (errMsg, tb) match {
          case (Some(e), Some(t)) => Some(s"$e\n$t".take(4000))
          case (Some(e), None)    => Some(e)
          case _                  => None
        }
        DryRunResult(ok = ok, rowsSeen = rowsSeen, samples = samples, error = combinedError)
      } catch {
        case _: Throwable =>
          DryRunResult(
            ok = false,
            rowsSeen = 0,
            samples = Seq.empty,
            error = Some(s"dry-run output was not valid JSON: ${rawOut.take(800)}")
          )
      }
    } finally {
      Try(Files.deleteIfExists(tmpFile))
    }
  }

  // ---------------------------------------------------------------------------
  // Sample reading
  // ---------------------------------------------------------------------------

  private case class SamplePreview(text: String, hex: String, kind: String)

  private def buildSample(uri: URI, extension: String): (SamplePreview, Array[Byte]) = {
    val ext = extension.toLowerCase
    val isPdf = ext == "pdf"
    val textish = Set("csv", "tsv", "json", "jsonl", "ndjson", "log", "txt", "html",
      "htm", "xml", "yaml", "yml", "md")
    val isText = textish.contains(ext)

    if (isPdf) {
      val text = Try(extractPdfText(uri, maxPages = 2)).toOption.getOrElse("")
      // For hash + audit, capture the first chunk of raw bytes.
      val bytes = Try(readBytes(uri, BinarySampleByteCount)).getOrElse(Array.emptyByteArray)
      val preview = SamplePreview(
        text = if (text.nonEmpty) text else lossyDecode(bytes),
        hex = hexPreview(bytes),
        kind = "pdf"
      )
      (preview, bytes)
    } else if (isText) {
      val bytes = readBytes(uri, SampleByteCount)
      (SamplePreview(text = lossyDecode(bytes), hex = "", kind = "text"), bytes)
    } else {
      val bytes = readBytes(uri, BinarySampleByteCount)
      (SamplePreview(text = lossyDecode(bytes), hex = hexPreview(bytes), kind = "binary"), bytes)
    }
  }

  private def readBytes(uri: URI, maxBytes: Int): Array[Byte] = {
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    try {
      val buffer = new Array[Byte](maxBytes)
      var totalRead = 0
      var lastRead = 0
      while (totalRead < buffer.length && {
               lastRead = stream.read(buffer, totalRead, buffer.length - totalRead)
               lastRead
             } > 0) {
        totalRead += lastRead
      }
      if (totalRead == buffer.length) buffer else buffer.take(totalRead)
    } finally stream.close()
  }

  /** Extract text from the first N pages of a PDF using Apache PDFBox 3.x. */
  private def extractPdfText(uri: URI, maxPages: Int): String = {
    try {
      val file = DocumentFactory.openReadonlyDocument(uri).asFile()
      val doc = org.apache.pdfbox.Loader.loadPDF(file)
      try {
        val stripper = new org.apache.pdfbox.text.PDFTextStripper()
        stripper.setStartPage(1)
        stripper.setEndPage(math.min(maxPages, doc.getNumberOfPages))
        stripper.getText(doc)
      } finally doc.close()
    } catch {
      case _: Throwable => ""
    }
  }

  private def lossyDecode(bytes: Array[Byte]): String =
    new String(bytes, StandardCharsets.UTF_8)

  private def hexPreview(bytes: Array[Byte]): String = {
    val limit = math.min(bytes.length, 256)
    bytes.take(limit).map(b => f"${b & 0xff}%02x").mkString(" ")
  }

  private def sha256Hex(bytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(bytes).map(b => f"${b & 0xff}%02x").mkString
  }

  private def extractExtension(name: String): String = {
    val idx = name.lastIndexOf('.')
    if (idx < 0 || idx == name.length - 1) "" else name.substring(idx + 1)
  }

  // ---------------------------------------------------------------------------
  // Prompt + LLM
  // ---------------------------------------------------------------------------

  private def buildPrompt(
      displayName: String,
      extension: String,
      isFolder: Boolean,
      fileCount: Int,
      userHint: String,
      samplePreview: SamplePreview,
      previousCode: String,
      previousError: String
  ): String = {
    val sb = new StringBuilder
    sb.append("You generate Python source-operator code for the Texera dataflow system.\n\n")
    sb.append("OUTPUT FORMAT — return ONLY a JSON object with this exact shape:\n")
    sb.append("{\n")
    sb.append("  \"code\": \"<python source>\",\n")
    sb.append("  \"tables\": [\n")
    sb.append("    {\"name\": \"<snake_case_name>\",\n")
    sb.append("     \"description\": \"<one-line: which physical tables you grouped and why>\",\n")
    sb.append("     \"columns\": [{\"name\": \"<col>\", \"type\": \"<TYPE>\"}, ...]}\n")
    sb.append("  ]\n")
    sb.append("}\n\n")
    sb.append("Permitted column types (lowercase): string, integer, long, double, boolean, timestamp, binary.\n\n")
    sb.append("TABLE-GROUPING RULES (CRITICAL — read carefully):\n")
    sb.append("- DO NOT produce a generic catch-all table (like `pdf_tables` with columns `page,table_index,col_0,col_1,...`). That defeats the purpose of this operator. The downstream user wants typed, semantically-named tables they can chart and analyze directly.\n")
    sb.append("- Instead, READ the sample text to identify real, business-meaningful tables. Name each table after WHAT IT REPRESENTS (e.g., `revenue`, `headcount`, `expenses`), not after where it appeared (e.g., not `table_on_page_2`).\n")
    sb.append("- Name columns after their semantic meaning (e.g., `month`, `region`, `revenue_usd`), NOT positional indices (`col_0`, `col_1`).\n")
    sb.append("- MERGE physical tables that share schema and meaning (e.g., same revenue table appearing on multiple pages, quarterly results repeated per quarter, rows continuing across page breaks) into ONE TableSpec with combined rows. Add a discriminator column if the merged source needs disambiguation (e.g., `quarter`, `year`, `region`).\n")
    sb.append("- KEEP separate any physically-similar but semantically-different tables (e.g., a 'revenue' table and a 'headcount' table may both have a `category`+`number` shape but mean different things — emit two TableSpecs).\n")
    sb.append("- For each TableSpec, write a one-line `description` that says which physical tables you grouped and why.\n")
    sb.append("- If the file legitimately has only ONE logical table, return a single-entry list — but still name it semantically, not generically.\n\n")
    sb.append("CODE SHAPE — your `code` MUST follow this skeleton EXACTLY:\n")
    sb.append("```python\n")
    sb.append("from pytexera import *\n")
    sb.append("import pdfplumber  # or other libs below\n\n")
    sb.append("class GenerateOperator(UDFSourceOperator):\n")
    sb.append("    @overrides\n")
    sb.append("    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:\n")
    sb.append("        file_uri = \"__TEXERA_FILE_URI__\"\n")
    sb.append("        # ... open and parse the file ...\n")
    sb.append("        for row in rows_of_table_a:\n")
    sb.append("            yield {\"__table__\": \"<table_a_name>\", \"col1\": ..., \"col2\": ...}\n")
    sb.append("        for row in rows_of_table_b:\n")
    sb.append("            yield {\"__table__\": \"<table_b_name>\", \"colA\": ..., \"colB\": ...}\n")
    sb.append("```\n\n")
    sb.append("RULES for the code:\n")
    sb.append("- Use the literal token `__TEXERA_FILE_URI__` for the file path. The backend substitutes it with an absolute local path before execution. DO NOT wrap it in any other path manipulation.\n")
    sb.append("- Yield **plain Python dicts**, NOT `Tuple(...)` objects. The framework accepts dicts. e.g.: `yield {\"__table__\": \"revenue\", \"month\": \"Jan\", ...}`.\n")
    sb.append("- Tag EVERY yielded row with a `__table__` key matching one of the `tables[*].name` entries you declared.\n")
    sb.append("- The dict keys (other than `__table__`) MUST match the column names you declared in `tables[*].columns[*].name` exactly. Don't introduce columns not declared in the schema.\n")
    sb.append("- Cast numeric values to the declared type: `int(...)` for integer/long, `float(...)` for double. Strip commas from formatted numbers (\"1,234\" → 1234). Wrap each cast in try/except and skip the row on failure rather than crashing the whole operator.\n")
    sb.append("- For PDF tables: use `pdfplumber`. Iterate `page.extract_tables()` and check the header row to decide which logical table you're looking at — don't rely on page numbers alone (a table might shift between pages across files).\n")
    sb.append("- Skip rows that are missing required fields (e.g., empty cells, header echoes mid-table) rather than yielding partial rows.\n")
    sb.append("- Available libraries (already installed): pdfplumber, pypdf, pandas, openpyxl, lxml, beautifulsoup4, json, csv, re. DO NOT use network libraries.\n")
    if (isFolder) {
      sb.append("- This is a FOLDER input (")
        .append(fileCount)
        .append(" files of the same kind). `file_uri` will be a DIRECTORY path, not a file path. DO NOT call single-file readers such as `pdfplumber.open(file_uri)` directly. Iterate over every file first — for example `for path in pathlib.Path(file_uri).iterdir():` — then open each file and apply the same parser to it.\n")
    }
    sb.append("\n")
    sb.append("---- FILE CONTEXT ----\n")
    sb.append("File: ").append(displayName).append("\n")
    if (extension.nonEmpty) sb.append("Extension: ").append(extension).append("\n")
    sb.append("Is folder: ").append(isFolder).append("\n")
    if (userHint.nonEmpty) sb.append("User hint: ").append(userHint).append("\n")
    sb.append("\n")
    sb.append("---- SAMPLE (").append(samplePreview.kind).append(") ----\n")
    // Cap the sample text. Claude Haiku 4.5 reads through context linearly, so trimming
    // here directly cuts generation time. 6KB is enough for a couple of pages of PDF text
    // or several thousand lines of CSV/JSON sample.
    sb.append(truncate(samplePreview.text, 6000)).append("\n")
    if (samplePreview.hex.nonEmpty) {
      sb.append("\n---- HEX PREVIEW (first 256 bytes) ----\n")
      sb.append(samplePreview.hex).append("\n")
    }
    if (previousCode.nonEmpty || previousError.nonEmpty) {
      sb.append("\n---- ITERATIVE REPAIR ----\n")
      sb.append("Your previous attempt failed. Fix the code based on the error below.\n")
      if (previousError.nonEmpty) {
        sb.append("Error:\n").append(truncate(previousError, 4000)).append("\n")
      }
      if (previousCode.nonEmpty) {
        sb.append("Previous code:\n").append(truncate(previousCode, 8000)).append("\n")
      }
    }
    sb.append("\nReturn ONLY the JSON object. No prose, no markdown fences.\n")
    sb.toString
  }

  private def truncate(s: String, max: Int): String =
    if (s.length <= max) s else s.substring(0, max) + s"\n... [truncated ${s.length - max} chars]"

  private def callLLM(model: String, prompt: String): String = {
    // Note: we don't send response_format={"type":"json_object"} because Claude (the default
    // on this LiteLLM proxy) doesn't natively support it. We rely on prompt-only JSON
    // instructions + stripJsonFences in the parser.
    val requestBody = Json.obj(
      "model" -> model,
      "max_tokens" -> MaxTokens,
      "messages" -> Json.arr(
        Json.obj(
          "role" -> "user",
          "content" -> prompt
        )
      )
    )

    val response = Unirest
      .post(s"${LLMConfig.baseUrl}/chat/completions")
      .header("Authorization", s"Bearer ${LLMConfig.masterKey}")
      .header("Content-Type", "application/json")
      .connectTimeout(ConnectTimeoutMs)
      .socketTimeout(SocketTimeoutMs)
      .body(Json.stringify(requestBody))
      .asString()

    if (response.getStatus >= 400) {
      throw new WebApplicationException(
        s"LLM call failed (${response.getStatus}): ${response.getBody}",
        response.getStatus
      )
    }

    val body = Json.parse(response.getBody)
    (body \ "choices" \ 0 \ "message" \ "content").asOpt[String].getOrElse {
      throw new WebApplicationException(
        s"Unexpected LLM response (no choices[0].message.content): ${response.getBody}",
        502
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Parsing the LLM output
  // ---------------------------------------------------------------------------

  private case class ParsedLlm(code: String, tables: List[ParsedTable])
  private case class ParsedTable(name: String, description: String, columns: List[LLMSourceColumn])

  private def parseLLMResponse(rawContent: String): ParsedLlm = {
    val cleaned = stripJsonFences(rawContent.trim)
    val node = try Json.parse(cleaned)
    catch {
      case _: Throwable =>
        throw new WebApplicationException(
          s"LLM did not return valid JSON. Content: ${truncate(rawContent, 1000)}",
          502
        )
    }
    val code = (node \ "code").asOpt[String].getOrElse {
      throw new WebApplicationException("LLM response missing 'code' field.", 502)
    }
    val tables = (node \ "tables").asOpt[Seq[JsObject]].getOrElse(Seq.empty).map { t =>
      val name = (t \ "name").asOpt[String].getOrElse("table")
      val description = (t \ "description").asOpt[String].getOrElse("")
      val columns = (t \ "columns").asOpt[Seq[JsObject]].getOrElse(Seq.empty).map { c =>
        val cname = (c \ "name").asOpt[String].getOrElse("column")
        val ctype = (c \ "type").asOpt[String].getOrElse("STRING").toUpperCase
        LLMSourceColumn(cname, normalizeType(ctype))
      }.toList
      ParsedTable(name, description, columns)
    }.toList
    if (tables.isEmpty) {
      throw new WebApplicationException("LLM response had no tables.", 502)
    }
    ParsedLlm(code, tables)
  }

  // Texera's AttributeType serializes lowercase via @JsonValue (see AttributeType.java),
  // so persisted values MUST be lowercase or the property panel's enum dropdown won't match.
  private val AllowedTypes = Set("string", "integer", "long", "double", "boolean", "timestamp", "binary")
  private def normalizeType(t: String): String = {
    val lower = t.toLowerCase
    if (AllowedTypes.contains(lower)) lower
    else lower match {
      case "str"                                       => "string"
      case "int" | "int32"                             => "integer"
      case "int64" | "bigint"                          => "long"
      case "float" | "float64" | "numeric" | "decimal" => "double"
      case "bool"                                      => "boolean"
      case "datetime" | "date" | "time"                => "timestamp"
      case "bytes" | "blob"                            => "binary"
      case _                                           => "string"
    }
  }

  private def stripJsonFences(s: String): String = {
    val fenced = "(?s)^```(?:json)?\\s*(.*?)\\s*```$".r
    s match {
      case fenced(inner) => inner
      case _             => s
    }
  }

  // ---------------------------------------------------------------------------
  // Python syntax validation
  // ---------------------------------------------------------------------------

  /** Returns Some(error) if the code fails to parse, None if it's valid OR if no python binary is available. */
  private def validatePython(code: String): Option[String] = {
    val tmpFile = Files.createTempFile("texera-llm-source-", ".py")
    try {
      Files.write(tmpFile, code.getBytes(StandardCharsets.UTF_8))
      val pythonBinary = findPythonBinary().getOrElse(return None)
      val cmd = Seq(
        pythonBinary,
        "-c",
        s"import ast,sys; ast.parse(open(${pyStringLiteral(tmpFile)}).read())"
      )
      val stderr = new StringBuilder
      val logger = ProcessLogger(_ => (), line => stderr.append(line).append('\n'))
      val exit = Try(cmd.!(logger)).getOrElse(Int.MinValue)
      if (exit == 0) None
      else if (exit == Int.MinValue) None // subprocess couldn't run — treat as skipped, not failure
      else Some(if (stderr.nonEmpty) stderr.toString.trim else s"python exited with code $exit")
    } catch {
      case _: java.io.IOException => None
    } finally {
      Try(Files.deleteIfExists(tmpFile))
    }
  }

  /**
   * Prefer the interpreter configured for real Python workers so dry-runs exercise the same
   * dependency environment as workflow execution. The fallback probes keep local development
   * usable when python.path is intentionally left blank or points to a stale binary.
   */
  private[resource] def pythonBinaryCandidates: Seq[String] =
    (Seq(PythonUtils.getPythonExecutable) ++
      Seq("python3", "python", "/opt/anaconda3/bin/python", "/usr/bin/python3", "/usr/local/bin/python3"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct

  /** Probe for a working python binary. Cached across requests after first call. */
  private def findPythonBinary(): Option[String] = {
    cachedPythonBinary match {
      case Some(value) => value
      case None =>
        val found = pythonBinaryCandidates.find { bin =>
          Try(Seq(bin, "-c", "0").!(ProcessLogger(_ => (), _ => ()))).toOption.contains(0)
        }
        cachedPythonBinary = Some(found)
        found
    }
  }

  private def pyStringLiteral(p: Path): String = "'" + p.toAbsolutePath.toString.replace("'", "\\'") + "'"
}
