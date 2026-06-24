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

package org.apache.texera.amber.operator.huggingFace

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.huggingFace.codegen.{
  AudioTaskCodegen,
  CodegenContext,
  MediaGenCodegen,
  QaRankingCodegen,
  TextGenCodegen
}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HuggingFaceInferenceOpDescSpec extends AnyFlatSpec with Matchers {

  private def makeDesc(
      token: EncodableString = "token",
      modelId: EncodableString = "Qwen/Qwen2.5-72B-Instruct",
      promptColumn: EncodableString = "prompt",
      task: EncodableString = "text-generation",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      maxNewTokens: Int = 256,
      temperature: Double = 0.7,
      resultColumn: EncodableString = "hf_response",
      imageInput: EncodableString = "",
      inputImageColumn: EncodableString = "",
      audioInput: EncodableString = "",
      inputAudioColumn: EncodableString = "",
      contextColumn: EncodableString = "",
      candidateLabels: EncodableString = "",
      sentencesColumn: EncodableString = ""
  ): HuggingFaceInferenceOpDesc = {
    val desc = new HuggingFaceInferenceOpDesc()
    desc.hfApiToken = token
    desc.modelId = modelId
    desc.promptColumn = promptColumn
    desc.task = task
    desc.systemPrompt = systemPrompt
    desc.maxNewTokens = maxNewTokens
    desc.temperature = temperature
    desc.resultColumn = resultColumn
    desc.imageInput = imageInput
    desc.inputImageColumn = inputImageColumn
    desc.audioInput = audioInput
    desc.inputAudioColumn = inputAudioColumn
    desc.contextColumn = contextColumn
    desc.candidateLabels = candidateLabels
    desc.sentencesColumn = sentencesColumn
    desc
  }

  "HuggingFaceInferenceOpDesc.operatorInfo" should
    "advertise the user-friendly name, HuggingFace group, and one input/output port" in {
    val info = (new HuggingFaceInferenceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Hugging Face"
    info.operatorGroupName shouldBe OperatorGroupConstants.HUGGINGFACE_GROUP
    info.inputPorts.size shouldBe 1
    info.outputPorts.size shouldBe 1
  }

  "generatePythonCode" should
    "fall back to the text-gen codegen on an unrecognized task (HF reports the real error at runtime)" in {
    // generatePythonCode must be total — never throw on arbitrary @JsonProperty
    // values — per the PythonCodeRawInvalidTextSpec contract. An unknown task
    // routes through TextGenCodegen, whose payload `if/else` hits the generic
    // `{"inputs": prompt_value}` branch at runtime.
    val code = makeDesc(task = "not-a-real-task").generatePythonCode()
    code should include("""payload = {"inputs": prompt_value}""")
  }

  it should "emit a ProcessTableOperator that initializes config in open()" in {
    val code = makeDesc().generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator):")
    code should include("def open(self):")
    // User-input strings are decoded at runtime, not embedded as literals.
    code should include("self.HF_API_TOKEN = self.decode_python_template(")
    code should include("self.MODEL_ID = self.decode_python_template(")
    code should include("self.PROMPT_COLUMN = self.decode_python_template(")
    code should include("self.TASK = self.decode_python_template(")
    code should include("self.SYSTEM_PROMPT = self.decode_python_template(")
  }

  it should "wire the text-gen payload and response parse correctly" in {
    val code = makeDesc().generatePythonCode()
    // Payload — chat-completions shape against the configured model + system prompt.
    code should include("self.MODEL_ID")
    code should include("self.SYSTEM_PROMPT")
    code should include("self.MAX_NEW_TOKENS")
    code should include("self.TEMPERATURE")
    // Parse — text-gen pulls choices[0].message.content out of the response.
    code should include("""body["choices"][0]["message"]["content"]""")
  }

  it should
    "emit a runtime check that rejects malformed MODEL_ID values before any HF URL is built" in {
    val code = makeDesc().generatePythonCode()
    // Pattern that fences MODEL_ID to org/model-name (allowing org/model-name/revision).
    code should include("_HF_MODEL_ID_PATTERN = re.compile(")
    // Runtime fail-fast inside process_table — happens before _resolve_providers
    // composes the URL, so a malformed value never escapes into a request.
    code should include("if not _HF_MODEL_ID_PATTERN.match(")
    code should include("raise ValueError(")
    code should include("Invalid Hugging Face model ID")
  }

  it should "not leak raw user-input strings into the generated Python source" in {
    // Sentinel value chosen to be distinctive and non-overlapping with anything
    // else in the template. If our encoding regressed back to raw literals
    // (e.g. `MODEL_ID = "MARKER_zXyq42"`), this assertion would fail.
    val marker = "MARKER_zXyq42"
    val code =
      makeDesc(modelId = marker, promptColumn = marker, token = marker).generatePythonCode()
    code should not include marker
  }

  it should "clamp maxNewTokens into the 1-4096 range" in {
    makeDesc(maxNewTokens = -5).generatePythonCode() should include(
      "self.MAX_NEW_TOKENS = 1"
    )
    makeDesc(maxNewTokens = 99999).generatePythonCode() should include(
      "self.MAX_NEW_TOKENS = 4096"
    )
  }

  it should "clamp temperature into the 0.0-2.0 range" in {
    makeDesc(temperature = -1.0).generatePythonCode() should include(
      "self.TEMPERATURE = 0.0"
    )
    makeDesc(temperature = 5.0).generatePythonCode() should include(
      "self.TEMPERATURE = 2.0"
    )
  }

  it should "tolerate null @JsonProperty values and fall back to safe defaults" in {
    // Every user-input field can land as null when the JSON deserializer is
    // handed a workflow that omits the field. generatePythonCode must not
    // throw on any combination — and the generated Python must still parse.
    val desc = new HuggingFaceInferenceOpDesc()
    desc.hfApiToken = null
    desc.modelId = null
    desc.promptColumn = null
    desc.systemPrompt = null
    desc.resultColumn = null
    desc.task = null
    desc.maxNewTokens = null
    desc.temperature = null
    desc.imageInput = null
    desc.inputImageColumn = null
    desc.audioInput = null
    desc.inputAudioColumn = null
    desc.contextColumn = null
    desc.candidateLabels = null
    desc.sentencesColumn = null
    val code = desc.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator):")
    code should include("def open(self):")
    // System-prompt default is the empty-string sentinel (no fallback string
    // injected) but the operator class still initializes the constant.
    code should include("self.SYSTEM_PROMPT = ")
    // maxNewTokens null path defaults to 256.
    code should include("self.MAX_NEW_TOKENS = 256")
    // temperature null path defaults to 0.7.
    code should include("self.TEMPERATURE = 0.7")
  }

  "TextGenCodegen" should "advertise text-generation as its canonical task" in {
    TextGenCodegen.task shouldBe "text-generation"
  }

  it should
    "emit payload and parse snippets that don't depend on the CodegenContext" in {
    // For text-generation, the codegen's only inputs to Python are static
    // strings referencing self.* attributes — exercising both methods
    // confirms they don't accidentally consume ctx fields (a future
    // refactor regression would surface here).
    val ctx = CodegenContext(
      hfApiToken = "irrelevant",
      modelId = "irrelevant",
      promptColumn = "irrelevant",
      resultColumn = "irrelevant",
      task = "irrelevant",
      systemPrompt = "irrelevant",
      safeMaxTokens = 0,
      safeTemp = 0.0
    )
    TextGenCodegen.payloadPython(ctx) should include("self.MODEL_ID")
    TextGenCodegen.parsePython(ctx) should include("""body["choices"][0]["message"]["content"]""")
  }

  "image task family" should
    "route image-only tasks through ImageTaskCodegen (raw binary payload + image headers)" in {
    val code =
      makeDesc(task = "image-classification", inputImageColumn = "img").generatePythonCode()
    code should include("self.IMAGE_INPUT = ")
    code should include("self.INPUT_IMAGE_COLUMN = ")
    code should include("if task in image_only_tasks:")
    code should include("payload = current_image_bytes")
    code should include("use_raw_binary_body = True")
    code should include("raw_binary_headers = image_headers")
    // image bytes resolution + image content-type response handling exist
    code should include("self._read_image_input()")
    code should include("self._read_binary_value")
    code should include("self._compress_image_bytes")
    code should include("""if content_type.startswith("image/"):""")
  }

  it should
    "not read arbitrary worker-filesystem paths for image inputs (SSRF/LFI hardening)" in {
    // Opening an arbitrary path from the worker filesystem would let a workflow
    // exfiltrate any file (e.g. /etc/passwd) via the inference call. Image inputs
    // must be data URLs, http(s) URLs, rendered HTML, or raw/base64 bytes only —
    // never a path passed to open().
    val code = makeDesc(task = "image-classification", inputImageColumn = "img")
      .generatePythonCode()
    // The removed filesystem-read branches must not reappear.
    code should not include "open(image_input"
    code should not include "os.path.isfile(image_input)"
    code should not include "os.path.exists(image_input)"
    code should not include "if os.path.exists(val) and os.path.isfile(val):"
    // Unsupported image inputs are rejected with a clear error instead.
    code should include("Unsupported image input")
  }

  it should "route VQA / document-QA through ImageTaskCodegen (base64 image + question payload)" in {
    val code = makeDesc(task = "visual-question-answering").generatePythonCode()
    code should include(
      """elif task in ("visual-question-answering", "document-question-answering"):"""
    )
    code should include("self._image_input_as_base64(current_image_bytes)")
    code should include(""""question": prompt_value""")
  }

  it should
    "emit single-backslash regex/whitespace escapes in the HTML->image helpers" in {
    // The HTML->image helpers came from the original monolith where, inside a
    // raw triple-quoted Scala string, "\\n"/"\\." emit DOUBLE backslashes to
    // Python. That makes the base64 char class match a literal backslash+n
    // instead of a newline, and makes the Plotly detection regex require a
    // literal backslash before "newPlot" (so it never matches). The generated
    // Python must contain single-backslash forms.
    val code = makeDesc(task = "image-to-text", inputImageColumn = "img").generatePythonCode()

    // base64 char class allows real newlines/CR; strip uses real newline chars.
    code should include("""[A-Za-z0-9+/\n\r =]""")
    code should include(""".replace("\n", "").replace("\r", "")""")
    // Plotly detection regex uses real regex escapes.
    code should include("""r"Plotly\.(?:newPlot|react)\s*\(\s*"""")
    // whitespace-skip set contains real whitespace chars.
    code should include("""in " ,\n\r\t"""")

    // The broken double-backslash forms must NOT reappear.
    code should not include """[A-Za-z0-9+/\\n\\r =]"""
    code should not include """Plotly\\.(?:newPlot|react)"""
    code should not include """in " ,\\n\\r\\t""""
  }

  it should "harden remote URL fetches against SSRF (https-only, private-IP block, size cap)" in {
    // Remote image/result URLs (user-provided or returned by a third-party
    // provider) are fetched through _fetch_remote_url, which enforces https,
    // rejects private/loopback/link-local/reserved/metadata addresses, and
    // caps the response size.
    val code = makeDesc(task = "image-to-image", inputImageColumn = "img").generatePythonCode()
    code should include("def _fetch_remote_url(self, url):")
    // https-only
    code should include("""if parsed.scheme != "https":""")
    // private / metadata IP blocking (169.254.169.254 is link-local)
    code should include("ip.is_private")
    code should include("ip.is_loopback")
    code should include("ip.is_link_local")
    code should include("Refusing to fetch from non-public address")
    // size cap
    code should include("MAX_REMOTE_FETCH_BYTES")
    code should include("Remote file exceeds the")
    // all remote fetch sites route through the helper (no raw requests.get on these URLs)
    code should include("_, data = self._fetch_remote_url(image_input)")
    code should include("_, data = self._fetch_remote_url(audio_input)")
    code should include("_, data = self._fetch_remote_url(val)")
    code should include("raw_content_type, data = self._fetch_remote_url(url)")
    code should not include "def _audio_url_to_data_url"
    code should not include "requests.get(audio_input"
    code should not include "os.path.exists(audio_input)"
    code should not include "open(audio_input"
  }

  it should "treat pandas NA sentinels (NaN, pd.NA, NaT) as missing in _read_binary_value" in {
    // isinstance(value, float) only catches float('nan'); pd.NA / NaT are not
    // floats and previously fell through to be str()-ified into bytes. The
    // guarded pd.isna check now catches all scalar NA sentinels.
    val code = makeDesc(task = "image-classification", inputImageColumn = "img")
      .generatePythonCode()
    code should include("if pd.isna(value):")
    code should include("except (TypeError, ValueError):")
    // The old float-only guard must be gone.
    code should not include "isinstance(value, float) and pd.isna(value)"
  }

  it should "not import the unused top-level urlparse in the generated script" in {
    val code = makeDesc().generatePythonCode()
    code should not include "from urllib.parse import urlparse\n"
    // The local aliased import is still used where needed.
    code should include("from urllib.parse import urlparse as _urlparse")
  }

  it should
    "convert Replicate terminal failed/canceled status into a synthetic 502 with surfaced error detail" in {
    // Replicate's polling endpoint returns HTTP 200 even when the prediction
    // itself terminally failed. Without this fix,
    // _post_with_fallback sees status 200 and process_table parses the
    // success-shape, silently emitting json.dumps(body) (raw error JSON)
    // into the result column instead of a readable error. We synthesize a
    // 502 with a top-level `error` field so the upstream non-200 path
    // surfaces the actual reason via _format_error.
    val code = makeDesc(task = "image-to-image").generatePythonCode()
    code should include("""if status == "succeeded":""")
    code should include("""if status in ("failed", "canceled"):""")
    code should include("Replicate prediction")
    code should include("poll_resp.status_code = 502")
    code should include("""body_json.get("error")""")
  }

  it should
    "convert Wavespeed terminal failed status into a synthetic 502 with surfaced error detail" in {
    // Same fix as Replicate, applied to Wavespeed's poll loop where the
    // pattern was `status in ("completed", "failed")` collapsing both
    // terminal states into a single `return poll_resp`. We now route
    // "failed" through the synthetic-502 path so the error reaches the
    // user instead of being parsed as a successful body.
    val code = makeDesc(task = "image-to-image").generatePythonCode()
    code should include("""if status == "completed":""")
    code should include("""if status == "failed":""")
    code should include("Wavespeed job failed")
  }

  it should
    "fail fast at runtime when zero-shot-image-classification has fewer than 2 candidate labels" in {
    // Without a dedicated candidateLabels field (lands in PR 5), zero-shot
    // reuses prompt_value as a comma-
    // separated list. Two failure modes the bare list comprehension hides
    // are both caught by the >= 2 check:
    //  1. Empty prompt column → labels = [] → HF API rejects
    //     candidate_labels: [] with an opaque 400.
    //  2. Missing prompt column → upstream falls back to "What is shown in
    //     this image?" (no comma) → labels = ["What is shown in this image?"],
    //     a single nonsense label that returns a useless 1.0 score.
    // Zero-shot classification needs >= 2 candidate labels to be meaningful,
    // so the fix raises ValueError before the request goes out and the user
    // sees a clear configuration error instead of a generic HTTP failure or
    // misleading 100%-confidence garbage.
    val code = makeDesc(task = "zero-shot-image-classification").generatePythonCode()
    code should include("if len(labels) < 2:")
    code should include("raise ValueError(")
    code should include("at least 2 candidate")
  }

  it should
    "extract base64 image from image+prompt dict payloads in _call_provider so third-party providers receive it" in {
    // Regression test: visual-question-answering,
    // document-question-answering, and zero-shot-image-classification build
    // dict payloads with use_raw_binary_body=False. Before the fix, when
    // those tasks routed off hf-inference to a third-party provider, the
    // top-of-_call_provider img_b64 stayed "" and the image was silently
    // dropped. The fix reads the base64 out of payload["inputs"]["image"]
    // (for VQA / doc-QA) or payload["inputs"] (for zero-shot-image-
    // classification) so every provider branch below sees a populated img_b64.
    val code = makeDesc(task = "visual-question-answering").generatePythonCode()
    // VQA / doc-QA: image at payload["inputs"]["image"].
    code should include("""isinstance(inputs, dict) and isinstance(inputs.get("image"), str)""")
    code should include("""img_b64 = inputs["image"]""")
    // Zero-shot-image-classification: image at payload["inputs"] directly.
    code should include(
      """elif task == "zero-shot-image-classification" and isinstance(inputs, str):"""
    )
    code should include("img_b64 = inputs")
  }

  it should "route image-text-to-text through chat completions with embedded base64 image" in {
    val code = makeDesc(task = "image-text-to-text").generatePythonCode()
    code should include("""elif task == "image-text-to-text":""")
    code should include("""data:image/png;base64,{img_b64}""")
    code should include("self.MODEL_ID")
  }

  it should "route image-to-image as raw binary and parse via _url_to_data_url on JSON response" in {
    val code = makeDesc(task = "image-to-image").generatePythonCode()
    code should include("""elif task == "image-to-image":""")
    code should include("self._url_to_data_url(")
  }

  it should
    "register all 9 image task strings under the dispatcher (image-only + image+prompt)" in {
    // Each image task should pull in ImageTaskCodegen's branch chain.
    val imageTasks = Seq(
      "image-classification",
      "object-detection",
      "image-segmentation",
      "image-to-text",
      "visual-question-answering",
      "document-question-answering",
      "zero-shot-image-classification",
      "image-text-to-text",
      "image-to-image"
    )
    imageTasks.foreach { t =>
      val code = makeDesc(task = t).generatePythonCode()
      code should include("if task in image_only_tasks:")
    }
  }

  "audio task family" should
    "route ASR and audio-classification through AudioTaskCodegen as raw binary payloads" in {
    val code =
      makeDesc(task = "automatic-speech-recognition", inputAudioColumn = "audio")
        .generatePythonCode()
    code should include("self.AUDIO_INPUT = ")
    code should include("self.INPUT_AUDIO_COLUMN = ")
    code should include(
      """audio_only_tasks = ("automatic-speech-recognition", "audio-classification")"""
    )
    code should include("payload = current_audio_bytes")
    code should include("raw_binary_headers = audio_headers")
    code should include("self._read_audio_input()")
    code should include(
      """"Content-Type": "application/octet-stream" if use_audio_column else self._get_audio_content_type()"""
    )
    code should include(
      """path = _urlparse(audio_input).path if audio_input.startswith("http") else audio_input"""
    )
    code should include(
      """audio_content_type = raw_binary_headers.get("Content-Type", "audio/mpeg")"""
    )
    code should include(
      """elif task in ("automatic-speech-recognition", "audio-classification") and img_b64:"""
    )
    code should not include "data:audio/wav;base64"
    code should include(
      """if content_type.startswith("audio/") or content_type.startswith("video/"):"""
    )
  }

  it should "route text-to-speech through AudioTaskCodegen and normalize audio URLs" in {
    val code = makeDesc(task = "text-to-speech").generatePythonCode()
    code should include("""elif task == "text-to-speech":""")
    code should include("""payload = {"inputs": prompt_value}""")
    code should include("self._url_to_data_url(")
    code should include(""""text-to-speech": "audio/mpeg"""")
    code should include("""".m4a": "audio/m4a"""")
    code should not include "_audio_url_to_data_url"
    code should include("data:audio/mpeg;base64")
  }

  it should "register all audio task strings under the dispatcher" in {
    AudioTaskCodegen.tasks should contain allOf (
      "automatic-speech-recognition",
      "audio-classification",
      "text-to-speech"
    )
    AudioTaskCodegen.tasks.foreach { t =>
      val code = makeDesc(task = t, inputAudioColumn = "audio").generatePythonCode()
      code should include("if task in audio_only_tasks:")
    }
  }

  "media generation task family" should
    "route text-to-image through MediaGenCodegen and parse URL or b64 responses as data URLs" in {
    val code = makeDesc(task = "text-to-image").generatePythonCode()
    code should include("if task not in image_tasks and task not in audio_only_tasks:")
    code should include("""payload = {"inputs": prompt_value}""")
    code should include("""if task == "text-to-image":""")
    code should include("self._url_to_data_url(")
    code should include("data:image/png;base64")
  }

  it should "route text-to-video through MediaGenCodegen and normalize remote video URLs" in {
    val code = makeDesc(task = "text-to-video").generatePythonCode()
    code should include("""elif task == "text-to-video":""")
    code should include("self._url_to_data_url(")
    code should include("video/mp4")
  }

  it should "register all media generation task strings under the dispatcher" in {
    MediaGenCodegen.tasks should contain allOf ("text-to-image", "text-to-video")
    MediaGenCodegen.tasks.foreach { t =>
      val code = makeDesc(task = t).generatePythonCode()
      code should include("""payload = {"inputs": prompt_value}""")
    }
  }

  "qa and ranking task family" should
    "route question-answering through QaRankingCodegen with context-column validation" in {
    val code = makeDesc(task = "question-answering", contextColumn = "context").generatePythonCode()
    code should include("self.CONTEXT_COLUMN = ")
    code should include("""if task == "question-answering":""")
    code should include("ctx_col = self.CONTEXT_COLUMN")
    code should include("Context column")
    code should include("""payload = {"inputs": {"question": prompt_value, "context": ctx_val}}""")
    code should include(
      """return body.get("answer", json.dumps(body)) if isinstance(body, dict) else json.dumps(body)"""
    )
  }

  it should "route table-question-answering with a precomputed table payload" in {
    val code = makeDesc(task = "table-question-answering").generatePythonCode()
    code should include("""if task == "table-question-answering":""")
    code should include("table_dict = {}")
    code should include("""payload = {"inputs": {"query": prompt_value, "table": table_dict}}""")
    code should include(
      """return body.get("answer", json.dumps(body)) if isinstance(body, dict) else json.dumps(body)"""
    )
  }

  it should "route zero-shot-classification with candidate labels" in {
    val code =
      makeDesc(task = "zero-shot-classification", candidateLabels = "positive,negative")
        .generatePythonCode()
    code should include("self.CANDIDATE_LABELS = ")
    code should include("""if task == "zero-shot-classification":""")
    code should include(
      "labels = [l.strip() for l in str(self.CANDIDATE_LABELS).split"
    )
    code should include("Candidate Labels are required for zero-shot-classification.")
    code should include("""elif task == "zero-shot-classification":""")
    code should include("labels = [l.strip() for l in str(self.CANDIDATE_LABELS).split")
    code should include(""""parameters": {"candidate_labels": labels}""")
  }

  it should "route sentence-similarity and text-ranking with sentences-column validation" in {
    Seq("sentence-similarity", "text-ranking").foreach { taskName =>
      val code = makeDesc(task = taskName, sentencesColumn = "sentences").generatePythonCode()
      code should include("self.SENTENCES_COLUMN = ")
      code should include("sent_col = self.SENTENCES_COLUMN")
      code should include("Sentences column")
      if (taskName == "sentence-similarity") {
        code should include("""elif task == "sentence-similarity":""")
        code should include(""""source_sentence": prompt_value""")
        code should include(""""sentences": sentences_list""")
      } else {
        code should include("""elif task == "text-ranking":""")
        code should include(""""query": prompt_value""")
        code should include(""""texts": sentences_list""")
      }
    }
  }

  it should "register all qa and ranking task strings under the dispatcher" in {
    QaRankingCodegen.tasks should contain allOf (
      "question-answering",
      "table-question-answering",
      "zero-shot-classification",
      "sentence-similarity",
      "text-ranking"
    )
    QaRankingCodegen.tasks.foreach { t =>
      val code = makeDesc(task = t, contextColumn = "context", sentencesColumn = "sentences")
        .generatePythonCode()
      code should include("""if task == "question-answering":""")
    }
  }

  "getOutputSchemas" should "add the result column as a STRING to the inherited schema" in {
    val desc = makeDesc(resultColumn = "answer")
    val inputSchema = Schema().add("prompt", AttributeType.STRING)
    val out = desc.getOutputSchemas(Map(PortIdentity(0) -> inputSchema))
    val outSchema = out(desc.operatorInfo.outputPorts.head.id)
    outSchema.getAttributeNames.contains("prompt") shouldBe true
    outSchema.getAttributeNames.contains("answer") shouldBe true
    outSchema.getAttribute("answer").getType shouldBe AttributeType.STRING
  }

  it should "fall back to the default 'hf_response' name when resultColumn is empty" in {
    val desc = makeDesc(resultColumn = "")
    val inputSchema = Schema().add("prompt", AttributeType.STRING)
    val out = desc.getOutputSchemas(Map(PortIdentity(0) -> inputSchema))
    val outSchema = out(desc.operatorInfo.outputPorts.head.id)
    outSchema.getAttributeNames.contains("hf_response") shouldBe true
  }
}
