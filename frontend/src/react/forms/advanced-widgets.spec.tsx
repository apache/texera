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

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SchemaForm } from "./schema-form";
import { createHuggingFaceCatalog, type HuggingFaceCatalog } from "./hugging-face";
import { compressImage, type ImagePreparation } from "./media-upload";
import { createPresetRepository } from "./presets";
import { ApiClient } from "../core/api-client";
import type { OperatorPropertySchema } from "./schema";

afterEach(cleanup);

const hfSchema: OperatorPropertySchema = {
  type: "object",
  properties: {
    task: { type: "string" },
    modelId: { type: "string", title: "Model" },
    promptColumn: { type: "string", title: "Prompt" },
    imageInput: { type: "string", title: "Image" },
    inputImageColumn: { type: "string", title: "Image column" },
    audioInput: { type: "string", title: "Audio" },
    inputAudioColumn: { type: "string", title: "Audio column" },
    temperature: { type: "number", title: "Temperature" },
  },
};
function hfCatalog(): HuggingFaceCatalog {
  return {
    tasks: vi.fn(async () => [
      { tag: "text-generation", label: "Text Generation" },
      { tag: "image-classification", label: "Image Classification" },
    ]),
    models: vi.fn(async () => ({ models: [{ id: "org/model", label: "org/model" }], truncated: false })),
  };
}

describe("HuggingFace operator form", () => {
  it("reads proxy headers and encodes task/search queries without sending credentials to a third party", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify([{ id: "org/model", label: "model" }]), {
          headers: { "X-Texera-Truncated": "true" },
        })
    );
    const catalog = createHuggingFaceCatalog({ fetch, getToken: () => "token" });
    expect(await catalog.models("text-generation", "a/b &c")).toMatchObject({
      truncated: true,
      models: [{ id: "org/model" }],
    });
    expect(fetch.mock.calls[0][0]).toBe("/api/huggingface/models?task=text-generation&search=a%2Fb+%26c");
    expect(new Headers(fetch.mock.calls[0][1].headers).get("Authorization")).toBe("Bearer token");
  });

  it("filters unsupported tasks, searches and paginates model choices", async () => {
    const service = hfCatalog();
    vi.mocked(service.tasks).mockResolvedValue([
      { tag: "text-generation", label: "Text Generation" },
      { tag: "unsupported", label: "Unsupported" },
    ]);
    vi.mocked(service.models).mockResolvedValue({
      truncated: false,
      models: Array.from({ length: 51 }, (_, index) => ({ id: `m${index}`, label: `m${index}` })),
    });
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={hfSchema}
        value={{ task: "text-generation", modelId: "" }}
        context={{ operatorType: "HuggingFace", huggingFaceCatalog: service }}
        onChange={onChange}
      />
    );
    await screen.findByRole("button", { name: "m0", exact: true });
    expect(screen.queryByRole("option", { name: "Unsupported" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.click(screen.getByRole("button", { name: "m50", exact: true }));
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ modelId: "m50" });
    fireEvent.change(screen.getByLabelText("Search models"), { target: { value: "m49" } });
    expect(screen.getByRole("button", { name: "m49", exact: true })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "m0", exact: true })).toBeNull();
  });

  it("uses supported fallback tasks after errors, retries models, and cancels stale truncated search", async () => {
    const service = hfCatalog();
    vi.mocked(service.tasks).mockRejectedValue(new Error("offline"));
    vi.mocked(service.models)
      .mockRejectedValueOnce(new Error("models offline"))
      .mockResolvedValue({ truncated: true, models: [{ id: "new", label: "new" }] });
    render(
      <SchemaForm
        schema={hfSchema}
        value={{ task: "text-generation", modelId: "" }}
        context={{ operatorType: "HuggingFace", huggingFaceCatalog: service }}
        onChange={vi.fn()}
      />
    );
    await screen.findByText("models offline");
    expect(screen.getByRole("option", { name: "Text Generation" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry models" }));
    await screen.findByRole("button", { name: "new", exact: true });
    fireEvent.change(screen.getByLabelText("Search models"), { target: { value: "query" } });
    await waitFor(() =>
      expect(service.models).toHaveBeenCalledWith("text-generation", "query", expect.any(AbortSignal))
    );
    expect(screen.getByText(/Results may be incomplete/)).toBeTruthy();
  });

  it("saves task-scoped fields when switching tasks and restores them on return", async () => {
    const service = hfCatalog();
    const onChange = vi.fn();
    const context = { operatorType: "HuggingFace", huggingFaceCatalog: service };
    const { rerender } = render(
      <SchemaForm
        schema={hfSchema}
        value={{ task: "text-generation", modelId: "old", promptColumn: "prompt", temperature: 0.2 }}
        context={context}
        onChange={onChange}
      />
    );
    await screen.findByRole("option", { name: "Image Classification" });
    await screen.findByRole("button", { name: "org/model" });
    fireEvent.change(screen.getByLabelText("Task"), { target: { value: "image-classification" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({
      task: "image-classification",
      modelId: "",
      temperature: 0.7,
      promptColumn: "",
    });
    rerender(
      <SchemaForm
        schema={hfSchema}
        value={onChange.mock.lastCall?.[0]}
        context={context}
        onChange={onChange}
      />
    );
    await screen.findByRole("button", { name: "org/model" });
    expect(screen.queryByLabelText("Prompt")).toBeNull();
    fireEvent.change(screen.getByLabelText("Task"), { target: { value: "text-generation" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({
      task: "text-generation",
      modelId: "old",
      promptColumn: "prompt",
      temperature: 0.2,
    });
  });
});

describe("HuggingFace media and UI UDF controls", () => {
  it("retains edits to other properties while media prepares and cancels when editing is disabled", async () => {
    let finish!: (value: string) => void;
    const image = vi.fn(
      async () =>
        new Promise<string>(resolve => {
          finish = resolve;
        })
    );
    const onChange = vi.fn();
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: { imageInput: { type: "string", title: "Image" }, name: { type: "string", title: "Name" } },
    };
    const context = { operatorType: "HuggingFace", mediaPreparation: { image, audio: vi.fn() } };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ task: "image-classification", name: "old" }}
        context={context}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Image"), {
      target: { files: [new File(["image"], "first.png", { type: "image/png" })] },
    });
    rerender(
      <SchemaForm
        schema={schema}
        value={{ task: "image-classification", name: "edited" }}
        context={context}
        onChange={onChange}
      />
    );
    await act(async () => finish("data:image/jpeg;base64,ok"));
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ name: "edited", imageInput: "data:image/jpeg;base64,ok" });
    onChange.mockClear();
    fireEvent.change(screen.getByLabelText("Image"), {
      target: { files: [new File(["image"], "second.png", { type: "image/png" })] },
    });
    rerender(
      <SchemaForm
        disabled
        schema={schema}
        value={{ task: "image-classification", name: "edited" }}
        context={context}
        onChange={onChange}
      />
    );
    await act(async () => finish("data:image/jpeg;base64,late"));
    expect(onChange).not.toHaveBeenCalled();
  });

  it("compresses images to the established dimensions/quality and rejects an oversized result", async () => {
    const renderImage = vi.fn((_image, width, height, quality) =>
      quality < 0.6 ? "data:image/jpeg;base64,ok" : "data:image/jpeg;base64," + "a".repeat(45000)
    );
    const options: ImagePreparation = {
      read: vi.fn(async () => "data:image/png;base64,raw"),
      load: vi.fn(async () => ({ width: 1024, height: 512 })),
      render: renderImage,
    };
    expect(await compressImage(new File(["image"], "photo.png", { type: "image/png" }), undefined, options)).toBe(
      "data:image/jpeg;base64,ok"
    );
    expect(renderImage.mock.calls[0].slice(1)).toEqual([512, 256, 0.75]);
    options.render = vi.fn(() => "data:image/jpeg;base64," + "a".repeat(45000));
    await expect(
      compressImage(new File(["image"], "photo.png", { type: "image/png" }), undefined, options)
    ).rejects.toThrow("smaller image");
  });

  it("validates media MIME and audio limits, stores a data URL, and clears the value", async () => {
    const onChange = vi.fn();
    const prepare = {
      image: vi.fn(async () => "data:image/jpeg;base64,ok"),
      audio: vi.fn(async () => "data:audio/wav;base64,ok"),
    };
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: { audioInput: { type: "string", title: "Audio" } },
    };
    const context = { operatorType: "HuggingFace", mediaPreparation: prepare };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ task: "audio-classification", audioInput: "" }}
        context={context}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Audio"), {
      target: { files: [new File(["no"], "bad.txt", { type: "text/plain" })] },
    });
    expect(screen.getByText("Choose an audio file.")).toBeTruthy();
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ audioInput: "" });
    fireEvent.change(screen.getByLabelText("Audio"), {
      target: { files: [new File(["ok"], "good.wav", { type: "audio/wav" })] },
    });
    await waitFor(() => expect(onChange.mock.lastCall?.[0]).toMatchObject({ audioInput: "data:audio/wav;base64,ok" }));
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        context={context}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear Audio" }));
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ audioInput: "" });
    const huge = new File(["x"], "huge.wav", { type: "audio/wav" });
    Object.defineProperty(huge, "size", { value: 25 * 1024 * 1024 + 1 });
    fireEvent.change(screen.getByLabelText("Audio"), { target: { files: [huge] } });
    expect(screen.getByText("Audio file is too large (max 25 MB).")).toBeTruthy();
  });

  it("cancels a superseded media selection instead of overwriting the newer file", async () => {
    let resolveOld!: (value: string) => void;
    const image = vi.fn(async (file: File) =>
      file.name === "old.png"
        ? new Promise<string>(resolve => {
            resolveOld = resolve;
          })
        : "data:image/jpeg;base64,new"
    );
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { imageInput: { type: "string", title: "Image" } } }}
        value={{ task: "image-classification" }}
        context={{ operatorType: "HuggingFace", mediaPreparation: { image, audio: vi.fn() } }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Image"), {
      target: { files: [new File(["old"], "old.png", { type: "image/png" })] },
    });
    fireEvent.change(screen.getByLabelText("Image"), {
      target: { files: [new File(["new"], "new.png", { type: "image/png" })] },
    });
    await waitFor(() =>
      expect(onChange.mock.lastCall?.[0]).toMatchObject({ imageInput: "data:image/jpeg;base64,new" })
    );
    await act(async () => resolveOld("data:image/jpeg;base64,old"));
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ imageInput: "data:image/jpeg;base64,new" });
    expect(image.mock.calls[0][1]?.aborted).toBe(true);
  });

  it("locks inferred UDF names/types, edits values and adds declarations through code synchronization", async () => {
    const onChange = vi.fn();
    const addUdfParameter = vi.fn().mockResolvedValue(undefined);
    render(
      <SchemaForm
        schema={{ type: "object", properties: { uiParameters: { type: "array", title: "Parameters" } } }}
        value={{ uiParameters: [{ attribute: { attributeName: "k", attributeType: "integer" }, value: "1" }] }}
        context={{ addUdfParameter }}
        onChange={onChange}
      />
    );
    expect((screen.getByLabelText("Parameter k name") as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByLabelText("Parameter k type") as HTMLInputElement).disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Parameter k value"), { target: { value: "7" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ uiParameters: [{ value: "7" }] });
    fireEvent.click(screen.getByRole("button", { name: "Add parameter", exact: true }));
    fireEvent.change(screen.getByLabelText("Parameter name"), { target: { value: "new_value" } });
    fireEvent.change(screen.getByLabelText("Parameter type"), { target: { value: "boolean" } });
    fireEvent.click(screen.getByRole("button", { name: "Insert parameter" }));
    await waitFor(() => expect(addUdfParameter).toHaveBeenCalledWith("new_value", "boolean"));
    expect(onChange).toHaveBeenCalledTimes(1);
  });
});

describe("operator presets", () => {
  it("serializes simultaneous saves so one preset cannot silently replace another", async () => {
    let saved = "[]";
    const fetch = vi.fn(async (_url, options) => {
      if (options.method === "PUT") {
        saved = options.body;
        return new Response(null, { status: 204 });
      }
      return new Response(saved);
    });
    const store = createPresetRepository(new ApiClient({ fetch }));
    await Promise.all([store.create("Scan", { name: "first" }), store.create("Scan", { name: "second" })]);
    expect(await store.list("Scan")).toEqual([{ name: "first" }, { name: "second" }]);
  });

  it("persists the existing user dictionary key, rejects malformed/duplicate presets and deletes an empty set", async () => {
    let saved = "[]";
    const fetch = vi.fn(async (_url, options) => {
      if (options.method === "PUT") {
        saved = options.body;
        return new Response(null, { status: 204 });
      }
      if (options.method === "DELETE") {
        saved = "[]";
        return new Response(null, { status: 204 });
      }
      return new Response(saved);
    });
    const store = createPresetRepository(new ApiClient({ fetch }));
    await store.create("MySQLSource", { host: "localhost", username: "reader" });
    expect(fetch.mock.calls[0][0]).toBe("/api/user/config/operator-MySQLSource");
    expect(await store.list("MySQLSource")).toEqual([{ host: "localhost", username: "reader" }]);
    await expect(store.create("MySQLSource", { host: "localhost", username: "reader" })).rejects.toThrow(
      "already exists"
    );
    await expect(store.create("MySQLSource", { host: "" })).rejects.toThrow("Fill out all preset fields");
    await store.remove("MySQLSource", { host: "localhost", username: "reader" });
    expect(fetch.mock.calls.at(-1)?.[1].method).toBe("DELETE");
  });
});
