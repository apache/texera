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

import { useEffect, useId, useRef, useState } from "react";
import { useOperatorFormContext } from "./operator-context";
import type { PropertyWidgetProps } from "./schema-form";

export interface PreparedImage {
  width: number;
  height: number;
}
export interface ImagePreparation {
  read(file: File, signal?: AbortSignal): Promise<string>;
  load(url: string, signal?: AbortSignal): Promise<PreparedImage>;
  render(image: PreparedImage, width: number, height: number, quality: number): string;
}
export interface MediaPreparation {
  image(file: File, signal?: AbortSignal): Promise<string>;
  audio(file: File, signal?: AbortSignal): Promise<string>;
}

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) throw new DOMException("Operation aborted", "AbortError");
}
export function readFileDataUrl(file: File, signal?: AbortSignal): Promise<string> {
  checkAbort(signal);
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    const cleanup = () => signal?.removeEventListener("abort", abort);
    const abort = () => {
      reader.abort();
      cleanup();
      reject(new DOMException("Operation aborted", "AbortError"));
    };
    reader.onload = () => {
      cleanup();
      if (typeof reader.result === "string") resolve(reader.result);
      else reject(new Error("Unexpected FileReader result"));
    };
    reader.onerror = () => {
      cleanup();
      reject(reader.error ?? new Error("File read failed"));
    };
    signal?.addEventListener("abort", abort, { once: true });
    reader.readAsDataURL(file);
  });
}
const browserImage: ImagePreparation = {
  read: readFileDataUrl,
  load(url, signal) {
    checkAbort(signal);
    return new Promise((resolve, reject) => {
      const image = new Image();
      const cleanup = () => signal?.removeEventListener("abort", abort);
      const abort = () => {
        image.onload = null;
        image.onerror = null;
        image.src = "";
        cleanup();
        reject(new DOMException("Operation aborted", "AbortError"));
      };
      image.onload = () => {
        cleanup();
        resolve(image);
      };
      image.onerror = () => {
        cleanup();
        reject(new Error("Image could not be loaded"));
      };
      signal?.addEventListener("abort", abort, { once: true });
      image.src = url;
    });
  },
  render(image, width, height, quality) {
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext("2d");
    if (!context) return "";
    context.drawImage(image as HTMLImageElement, 0, 0, width, height);
    return canvas.toDataURL("image/jpeg", quality);
  },
};

/** Retains the image operator's 45,000-character inline payload bound and JPEG reduction order. */
export async function compressImage(
  file: File,
  signal?: AbortSignal,
  preparation: ImagePreparation = browserImage
): Promise<string> {
  if (!file.type.startsWith("image/")) throw new Error("Choose an image file.");
  const source = await preparation.read(file, signal);
  checkAbort(signal);
  const image = await preparation.load(source, signal);
  checkAbort(signal);
  if (!(image.width > 0 && image.height > 0))
    throw new Error("Could not prepare this image. Try a smaller image file.");
  for (let dimension = 512; dimension >= 160; dimension = Math.floor(dimension * 0.75)) {
    const scale = Math.min(1, dimension / Math.max(image.width, image.height));
    const width = Math.max(1, Math.round(image.width * scale));
    const height = Math.max(1, Math.round(image.height * scale));
    for (let quality = 0.75; quality >= 0.35; quality -= 0.1) {
      checkAbort(signal);
      const value = preparation.render(image, width, height, quality);
      if (value.startsWith("data:image/") && value.length <= 45000) return value;
    }
  }
  throw new Error("Could not prepare this image. Try a smaller image file.");
}

export async function prepareAudio(file: File, signal?: AbortSignal): Promise<string> {
  if (!file.type.startsWith("audio/")) throw new Error("Choose an audio file.");
  if (file.size > 25 * 1024 * 1024) throw new Error("Audio file is too large (max 25 MB).");
  const value = await readFileDataUrl(file, signal);
  if (!value.startsWith("data:audio/")) throw new Error("Could not read this audio file.");
  return value;
}
const browserMedia: MediaPreparation = { image: compressImage, audio: prepareAudio };

export function MediaUploadField({
  schema,
  value,
  disabled,
  onChange,
  kind,
}: PropertyWidgetProps & { kind: "image" | "audio" }) {
  const preparation = useOperatorFormContext().mediaPreparation ?? browserMedia;
  const [error, setError] = useState<string>();
  const [name, setName] = useState("");
  const [loading, setLoading] = useState(false);
  const request = useRef<AbortController | undefined>(undefined);
  const emit = useRef(onChange);
  emit.current = onChange;
  const input = useRef<HTMLInputElement>(null);
  const id = useId();
  const label = schema.title ?? (kind === "image" ? "Image" : "Audio");
  const source = typeof value === "string" && value.startsWith(`data:${kind}/`) ? value : undefined;
  useEffect(() => () => request.current?.abort(), []);
  useEffect(() => {
    if (disabled) {
      request.current?.abort();
      setLoading(false);
    }
  }, [disabled]);
  const reset = () => {
    if (input.current) input.current.value = "";
    setName("");
    emit.current("");
  };
  return (
    <div className="media-upload-field">
      <label htmlFor={id}>{label}</label>
      <input
        id={id}
        ref={input}
        type="file"
        accept={`${kind}/*`}
        disabled={disabled}
        onChange={async event => {
          if (disabled) return;
          const file = event.currentTarget.files?.[0];
          if (!file) return;
          request.current?.abort();
          const controller = new AbortController();
          request.current = controller;
          setError(undefined);
          if (!file.type.startsWith(`${kind}/`)) {
            setError(`Choose an ${kind} file.`);
            reset();
            return;
          }
          if (kind === "audio" && file.size > 25 * 1024 * 1024) {
            setError("Audio file is too large (max 25 MB).");
            reset();
            return;
          }
          setLoading(true);
          try {
            const prepared = await preparation[kind](file, controller.signal);
            if (controller.signal.aborted) return;
            if (!prepared.startsWith(`data:${kind}/`) || (kind === "image" && prepared.length > 45000))
              throw new Error("Invalid media payload");
            setName(file.name);
            emit.current(prepared);
          } catch {
            if (!controller.signal.aborted) {
              setError(
                kind === "image"
                  ? "Could not prepare this image. Try a smaller image file."
                  : "Could not read this audio file."
              );
              reset();
            }
          } finally {
            if (!controller.signal.aborted) setLoading(false);
          }
        }}
      />
      {loading && <p role="status">Preparing {kind}…</p>}
      {source && (
        <div>
          {kind === "image" ? (
            <img
              src={source}
              alt="Uploaded Hugging Face task input"
            />
          ) : (
            <audio
              controls
              src={source}
            />
          )}
          <span>{name || (kind === "image" ? "Uploaded image" : "Selected audio")}</span>
          <button
            type="button"
            disabled={disabled}
            aria-label={`Clear ${label}`}
            onClick={() => {
              request.current?.abort();
              setLoading(false);
              setError(undefined);
              reset();
            }}>
            Clear
          </button>
        </div>
      )}
      {error && <p role="alert">{error}</p>}
    </div>
  );
}
