/**
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

import type { PropertyValidationContext } from "./schema";
export interface TaskPreview {
  kind: "image" | "video" | "audio" | "text";
  inputLabel?: string;
  outputLabel?: string;
  title?: string;
  body?: string;
  outputBody?: string;
  pills?: string[];
  assetSrc?: string;
}
// The original task-preview examples and bundled assets are retained verbatim.
const taskPreviews: Readonly<Record<string, TaskPreview>> = {
  "text-to-image": {
    kind: "image",
    inputLabel: "Text prompt",
    outputLabel: "Generated image",
    title: "Illustrated rooftop leap at sunset",
    body: "Prompt becomes a generated image preview.",
    assetSrc: "assets/sample-image.png",
  },
  "image-to-image": {
    kind: "image",
    inputLabel: "Source image",
    outputLabel: "Edited image",
    title: "Image transformation preview",
    body: "Image input produces a modified image result.",
    assetSrc: "assets/sample-image.png",
  },
  "text-to-video": {
    kind: "video",
    inputLabel: "Text prompt",
    outputLabel: "Generated video",
    title: "Prompt-based motion preview",
    body: "Prompt becomes a generated video clip.",
    assetSrc: "assets/sample-video.mp4",
  },
  "image-to-video": {
    kind: "video",
    inputLabel: "Source image",
    outputLabel: "Animated clip",
    title: "Image animation preview",
    body: "Image input becomes a short generated video.",
    assetSrc: "assets/sample-video.mp4",
  },
  "text-to-speech": {
    kind: "audio",
    inputLabel: "Text input",
    outputLabel: "Spoken audio",
    title: "Speech synthesis preview",
    body: "Text becomes an audio clip the user can play back.",
    assetSrc: "assets/sample-audio.wav",
  },
  "automatic-speech-recognition": {
    kind: "audio",
    inputLabel: "Audio input",
    outputLabel: "Transcript text",
    title: "Speech-to-text preview",
    body: "Uploaded audio is transcribed into plain text.",
    assetSrc: "assets/sample-audio.wav",
  },
  "audio-classification": {
    kind: "audio",
    inputLabel: "Audio input",
    outputLabel: "Labels and scores",
    title: "Audio tagging preview",
    body: "Uploaded audio returns classification labels.",
    assetSrc: "assets/sample-audio.wav",
  },
  "image-text-to-text": {
    kind: "image",
    inputLabel: "Image + text prompt",
    outputLabel: "Generated text",
    title: "Image-text-to-text preview",
    body: "The model reads an image and a text prompt to produce a response.",
    outputBody: "The image shows a person leaping across rooftops at sunset.",
    assetSrc: "assets/sample-image.png",
  },
  "image-classification": {
    kind: "image",
    inputLabel: "Image input",
    outputLabel: "Predicted labels",
    title: "Image classification preview",
    body: "The model assigns labels such as parkour, city, or action scene.",
    assetSrc: "assets/sample-image.png",
    pills: ["parkour", "cityscape", "action"],
  },
  "object-detection": {
    kind: "image",
    inputLabel: "Image input",
    outputLabel: "Detected objects",
    title: "Object detection preview",
    body: "The model returns detected objects and bounding boxes.",
    assetSrc: "assets/sample-image.png",
    pills: ["person", "building", "sky"],
  },
  "image-segmentation": {
    kind: "image",
    inputLabel: "Image input",
    outputLabel: "Segmented regions",
    title: "Segmentation preview",
    body: "The model separates the image into labeled regions.",
    assetSrc: "assets/sample-image.png",
    pills: ["foreground", "background", "subject"],
  },
  "image-to-text": {
    kind: "image",
    inputLabel: "Image input",
    outputLabel: "Caption text",
    title: "Captioning preview",
    body: "The model turns an uploaded image into a textual description.",
    outputBody: "A person leaps between rooftops above a downtown skyline at sunset.",
    assetSrc: "assets/sample-image.png",
  },
  "visual-question-answering": {
    kind: "image",
    inputLabel: "Image + question",
    outputLabel: "Answer text",
    title: "Visual question answering preview",
    body: "The model reads the image and answers the user question.",
    outputBody: "A person is jumping between two rooftops.",
    assetSrc: "assets/sample-image.png",
  },
  "document-question-answering": {
    kind: "image",
    inputLabel: "Document image + question",
    outputLabel: "Answer text",
    title: "Document QA preview",
    body: "The model extracts answers from a document image.",
    outputBody: "Invoice total: $248.90",
    assetSrc: "assets/sample-image.png",
  },
  "zero-shot-image-classification": {
    kind: "image",
    inputLabel: "Image + candidate labels",
    outputLabel: "Ranked labels",
    title: "Zero-shot image labeling preview",
    body: "Candidate labels are scored against the uploaded image.",
    assetSrc: "assets/sample-image.png",
    pills: ["parkour", "sports", "travel"],
  },
  "text-generation": {
    kind: "text",
    inputLabel: "Prompt",
    outputLabel: "Generated text",
    title: "Text generation preview",
    body: "Write a short action scene set above a crowded city skyline.",
    outputBody: "The hero vaulted between rooftops as the city lights came alive below.",
  },
  "text-classification": {
    kind: "text",
    inputLabel: "Text input",
    outputLabel: "Predicted label",
    title: "Text classification preview",
    body: "This launch update sounds confident and customer-focused.",
    pills: ["positive", "announcement"],
  },
  "token-classification": {
    kind: "text",
    inputLabel: "Text input",
    outputLabel: "Tagged spans",
    title: "Token classification preview",
    body: "Maya Chen visited New York yesterday.",
    pills: ["Maya Chen: PERSON", "New York: LOCATION"],
  },
  "question-answering": {
    kind: "text",
    inputLabel: "Question + context",
    outputLabel: "Answer span",
    title: "Question answering preview",
    body: "Question: Who led the launch?\nContext: Maya led the launch while Jordan handled analytics.",
    outputBody: "Maya",
  },
  "table-question-answering": {
    kind: "text",
    inputLabel: "Question + table",
    outputLabel: "Answer",
    title: "Table QA preview",
    body: "Question: Which month had the highest revenue?",
    outputBody: "March",
  },
  "zero-shot-classification": {
    kind: "text",
    inputLabel: "Text + candidate labels",
    outputLabel: "Ranked labels",
    title: "Zero-shot classification preview",
    body: "We need to accelerate onboarding for enterprise customers.",
    pills: ["business", "operations", "support"],
  },
  translation: {
    kind: "text",
    inputLabel: "Source text",
    outputLabel: "Translated text",
    title: "Translation preview",
    body: "Good morning, thanks for joining the call.",
    outputBody: "Buenos dias, gracias por unirte a la llamada.",
  },
  summarization: {
    kind: "text",
    inputLabel: "Long text",
    outputLabel: "Summary",
    title: "Summarization preview",
    body: "A long project update is compressed into a short summary.",
    outputBody: "The team shipped the release, fixed two regressions, and started the next milestone.",
  },
  "feature-extraction": {
    kind: "text",
    inputLabel: "Text input",
    outputLabel: "Embedding/vector output",
    title: "Feature extraction preview",
    body: "Input text is converted into a numeric representation.",
    pills: ["0.12", "-0.08", "0.44", "..."],
  },
  "fill-mask": {
    kind: "text",
    inputLabel: "Masked sentence",
    outputLabel: "Top completions",
    title: "Fill-mask preview",
    body: "The hero saved the [MASK].",
    pills: ["city", "day", "crowd"],
  },
  "sentence-similarity": {
    kind: "text",
    inputLabel: "Source + candidate sentences",
    outputLabel: "Similarity scores",
    title: "Sentence similarity preview",
    body: "Compare one sentence against several alternatives.",
    pills: ["0.93", "0.61", "0.22"],
  },
  "text-ranking": {
    kind: "text",
    inputLabel: "Query + candidate texts",
    outputLabel: "Ranked results",
    title: "Text ranking preview",
    body: "Candidate passages are ordered by relevance to the query.",
    pills: ["doc_2", "doc_5", "doc_1"],
  },
};
export function huggingFaceTaskPreview(task: unknown): TaskPreview | null {
  if (typeof task !== "string" || !task.trim()) return null;
  return (
    taskPreviews[task] ?? {
      kind: "text",
      inputLabel: "Task input",
      outputLabel: "Task output",
      title: task
        .split("-")
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(" "),
      body: "This task transforms the provided input into a model response.",
    }
  );
}
export function typeCastingAttributes(
  value: Readonly<Record<string, unknown>>,
  input: PropertyValidationContext["inputAttributes"]
) {
  const units = Array.isArray(value.typeCastingUnits) ? value.typeCastingUnits : [];
  const casts = new Map(
    units.flatMap(unit =>
      unit && typeof unit === "object" && typeof unit.attribute === "string" && typeof unit.resultType === "string"
        ? [[unit.attribute, unit.resultType] as const]
        : []
    )
  );
  return Object.values(input ?? {}).flatMap(attributes =>
    (attributes ?? []).map(attribute =>
      casts.has(attribute.attributeName)
        ? { ...attribute, attributeType: casts.get(attribute.attributeName)! }
        : attribute
    )
  );
}
export function OperatorSupplements({
  operatorType,
  value,
  inputAttributes,
}: {
  operatorType: string;
  value: Readonly<Record<string, unknown>>;
  inputAttributes?: PropertyValidationContext["inputAttributes"];
}) {
  if (operatorType === "TypeCasting")
    return (
      <table className="operator-type-casting">
        <thead>
          <tr>
            <th>Attribute Name</th>
            <th>Attribute Type</th>
          </tr>
        </thead>
        <tbody>
          {typeCastingAttributes(value, inputAttributes).map((attribute, index) => (
            <tr key={`${index}:${attribute.attributeName}`}>
              <td>{attribute.attributeName}</td>
              <td>{attribute.attributeType}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  const preview = operatorType === "HuggingFace" ? huggingFaceTaskPreview(value.task) : null;
  if (!preview) return null;
  return (
    <section
      className="operator-task-preview"
      aria-label="Task preview">
      <h3>Task Preview</h3>
      {preview.kind === "image" && (
        <img
          src={preview.assetSrc}
          alt="Hugging Face task preview"
        />
      )}
      {preview.kind === "video" && (
        <video
          src={preview.assetSrc}
          controls
          autoPlay
          loop
          muted
          playsInline
        />
      )}
      {preview.kind === "audio" && (
        <audio
          src={preview.assetSrc}
          controls
          preload="metadata"
        />
      )}
      <h4>{preview.title}</h4>
      <p>
        {preview.inputLabel} → {preview.outputLabel}
      </p>
      <p>{preview.body}</p>
      {preview.outputBody && <p>{preview.outputBody}</p>}
      {preview.pills?.map((pill, index) => (
        <span
          key={`${index}:${pill}`}
          className="operator-task-preview-pill">
          {pill}
        </span>
      ))}
    </section>
  );
}
