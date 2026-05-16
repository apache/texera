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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { UserDatasetFileRendererComponent, MIME_TYPES, getMimeType, inferColumnSchema } from "./user-dataset-file-renderer.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { DomSanitizer } from "@angular/platform-browser";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";
import { Router } from "@angular/router";
import { WorkflowPersistService } from "../../../../../../common/service/workflow-persist/workflow-persist.service";

describe("UserDatasetFileRendererComponent", () => {
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [UserDatasetFileRendererComponent, HttpClientTestingModule],
      providers: [
        DatasetService,
        NotificationService,
        WorkflowPersistService,
        { provide: Router, useValue: { navigate: vi.fn() } },
        {
          provide: DomSanitizer,
          useValue: {
            bypassSecurityTrustUrl: vi.fn((url: string) => url),
            bypassSecurityTrustResourceUrl: vi.fn((url: string) => url),
          },
        },
        ...commonTestProviders,
      ],
    });
    const fixture = TestBed.createComponent(UserDatasetFileRendererComponent);
    component = fixture.componentInstance;
  });

  describe("isPreviewSupported", () => {
    it("should return true for known MIME types", () => {
      expect(component.isPreviewSupported("image/jpeg")).toBe(true);
      expect(component.isPreviewSupported("application/pdf")).toBe(true);
      expect(component.isPreviewSupported("application/x-parquet")).toBe(true);
    });

    it("should return false only for unidentified binary (octet-stream)", () => {
      expect(component.isPreviewSupported(MIME_TYPES.OCTET_STREAM)).toBe(false);
    });
  });

  describe("getMimeType (extension-based fallback)", () => {
    it("should resolve common image extensions", () => {
      expect(getMimeType("photo.jpg")).toBe(MIME_TYPES.JPEG);
      expect(getMimeType("photo.PNG")).toBe(MIME_TYPES.PNG);
      expect(getMimeType("anim.gif")).toBe(MIME_TYPES.GIF);
    });

    it("should resolve xlsx separately from xls", () => {
      expect(getMimeType("data.xlsx")).toBe(MIME_TYPES.XLSX);
      expect(getMimeType("data.xls")).toBe(MIME_TYPES.MSEXCEL);
    });

    it("should resolve data format extensions", () => {
      expect(getMimeType("data.parquet")).toBe(MIME_TYPES.PARQUET);
      expect(getMimeType("data.arrow")).toBe(MIME_TYPES.ARROW);
      expect(getMimeType("data.feather")).toBe(MIME_TYPES.ARROW);
    });

    it("should return octet-stream for unknown extensions", () => {
      expect(getMimeType("file.xyz")).toBe(MIME_TYPES.OCTET_STREAM);
      expect(getMimeType("noextension")).toBe(MIME_TYPES.OCTET_STREAM);
    });
  });

  describe("detectMimeType (magic byte detection)", () => {
    it("should detect Parquet files from PAR1 magic bytes", async () => {
      const magic = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x00, 0x00, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.PARQUET);
    });

    it("should detect Arrow IPC files from ARROW1 magic bytes", async () => {
      const magic = new Uint8Array([0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.ARROW);
    });

    it("should detect JSON via text sniffing (object)", async () => {
      const blob = new Blob(['{"key": "value"}'], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.JSON);
    });

    it("should detect JSON via text sniffing (array)", async () => {
      const blob = new Blob(['[1, 2, 3]'], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.JSON);
    });

    it("should detect CSV via text sniffing", async () => {
      const blob = new Blob(["name,age,city\nAlice,30,LA\nBob,25,NY"], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.CSV);
    });

    it("should detect Markdown via text sniffing", async () => {
      const blob = new Blob(["# My Title\n\nSome content here"], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.MD);
    });

    it("should detect plain text when content is printable ASCII", async () => {
      const blob = new Blob(["Hello, world! This is plain text."], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.TXT);
    });

    it("should return octet-stream for unidentifiable binary", async () => {
      const binary = new Uint8Array([0x00, 0x01, 0x02, 0x80, 0xff, 0xfe, 0x7f, 0x03]);
      const blob = new Blob([binary]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.OCTET_STREAM);
    });

    it("should detect HDF5 from magic bytes (generic .h5)", async () => {
      const magic = new Uint8Array([0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob, "model.h5");
      expect(result).toBe(MIME_TYPES.HDF5);
    });

    it("should refine HDF5 to H5AD by extension", async () => {
      const magic = new Uint8Array([0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob, "scrna.h5ad");
      expect(result).toBe(MIME_TYPES.H5AD);
    });

    it("should refine HDF5 to H5SEURAT by extension", async () => {
      const magic = new Uint8Array([0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob, "pbmc.h5seurat");
      expect(result).toBe(MIME_TYPES.H5SEURAT);
    });

    it("should detect Python pickle from \\x80 + protocol byte", async () => {
      const magic = new Uint8Array([0x80, 0x04, 0x95, 0x00, 0x00, 0x00, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.PICKLE);
    });

    it("should detect NumPy .npy from magic bytes", async () => {
      const magic = new Uint8Array([0x93, 0x4e, 0x55, 0x4d, 0x50, 0x59, 0x01, 0x00, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.NPY);
    });

    it("should detect GGUF from magic bytes", async () => {
      const magic = new Uint8Array([0x47, 0x47, 0x55, 0x46, 0x03, 0x00, 0x00, 0x00]);
      const blob = new Blob([magic]);
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.GGUF);
    });

    it("should detect Safetensors via extension fallback", async () => {
      const opaque = new Uint8Array([0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
      const blob = new Blob([opaque]);
      const result = await component.detectMimeType(blob, "model.safetensors");
      expect(result).toBe(MIME_TYPES.SAFETENSORS);
    });

    it("should detect ONNX via extension fallback", async () => {
      const opaque = new Uint8Array([0x08, 0x07, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00]);
      const blob = new Blob([opaque]);
      const result = await component.detectMimeType(blob, "resnet.onnx");
      expect(result).toBe(MIME_TYPES.ONNX);
    });

    it("should detect VCF from header line", async () => {
      const blob = new Blob(["##fileformat=VCFv4.2\n##source=test\n"], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.VCF);
    });

    it("should detect FASTA from > prefix", async () => {
      const blob = new Blob([">seq1\nACGTACGT\n>seq2\nTGCATGCA\n"], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.FASTA);
    });

    it("should detect FASTQ from 4-line @/+ pattern", async () => {
      const blob = new Blob(["@read1\nACGT\n+\n!!!!\n@read2\nTGCA\n+\n!!!!\n"], { type: "text/plain" });
      const result = await component.detectMimeType(blob);
      expect(result).toBe(MIME_TYPES.FASTQ);
    });
  });

  describe("parser helpers", () => {
    it("should parse a NumPy v1.0 header", async () => {
      // Construct a minimal valid .npy v1 file: magic + version + uint16 header_len + ASCII header
      const headerText = "{'descr': '<f8', 'fortran_order': False, 'shape': (10, 256), }";
      const padded = headerText + " ".repeat(64 - (headerText.length % 64)) + "\n";
      const headerBytes = new TextEncoder().encode(padded);
      const headerLen = headerBytes.length;
      const buf = new Uint8Array(10 + headerLen);
      buf.set([0x93, 0x4e, 0x55, 0x4d, 0x50, 0x59, 0x01, 0x00], 0);
      buf[8] = headerLen & 0xff;
      buf[9] = (headerLen >> 8) & 0xff;
      buf.set(headerBytes, 10);
      const blob = new Blob([buf]);
      const result = await (component as any).parseNpyHeader(blob);
      expect(result?.dtype).toBe("<f8");
      expect(result?.shape).toEqual([10, 256]);
    });

    it("should parse a Safetensors header", async () => {
      const header = JSON.stringify({
        "layer.weight": { dtype: "F32", shape: [128, 64], data_offsets: [0, 32768] },
        "layer.bias": { dtype: "F32", shape: [128], data_offsets: [32768, 33280] },
        __metadata__: { format: "pt" },
      });
      const headerBytes = new TextEncoder().encode(header);
      const lenBytes = new Uint8Array(8);
      let len = headerBytes.length;
      for (let i = 0; i < 8; i++) {
        lenBytes[i] = len & 0xff;
        len = Math.floor(len / 256);
      }
      const blob = new Blob([lenBytes, headerBytes]);
      const result = await (component as any).parseSafetensorsHeader(blob);
      expect(result?.tensorCount).toBe(2);
      expect(result?.parameterCount).toBe(128 * 64 + 128);
      expect(result?.sampleNames).toEqual(["layer.weight", "layer.bias"]);
    });

    it("should infer column types from tabular sample data", () => {
      const rows = [
        ["Alice", "30", "75000.50", "true", "2024-01-15"],
        ["Bob", "25", "60000.00", "false", "2024-03-22"],
        ["Carol", "", "82000.75", "true", "2024-05-10"],
      ];
      const schema = inferColumnSchema(rows, 5);
      expect(schema.types).toEqual(["string", "integer", "double", "boolean", "date"]);
      expect(schema.nullCounts).toEqual([0, 1, 0, 0, 0]);
      expect(schema.samples).toEqual(["Alice", "30", "75000.50", "true", "2024-01-15"]);
    });

    it("should fall back to string for all-null columns", () => {
      const rows = [["a", ""], ["b", ""]];
      const schema = inferColumnSchema(rows, 2);
      expect(schema.types).toEqual(["string", "string"]);
      expect(schema.nullCounts).toEqual([0, 2]);
    });

    it("should expose canOpenInWorkflow whenever a filePath is set", () => {
      component.filePath = "/x/y/v1/data.csv";
      expect(component.canOpenInWorkflow).toBe(true);
      component.filePath = "/x/y/v1/model.safetensors";
      expect(component.canOpenInWorkflow).toBe(true);
    });

    it("should not expose canOpenInWorkflow when no file is selected", () => {
      component.filePath = "";
      expect(component.canOpenInWorkflow).toBe(false);
    });


    it("should parse a GGUF header", async () => {
      const buf = new Uint8Array(24);
      buf.set([0x47, 0x47, 0x55, 0x46], 0); // "GGUF"
      buf.set([0x03, 0x00, 0x00, 0x00], 4); // version 3
      buf.set([0xd2, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 8); // 722 tensors
      buf.set([0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 16); // 16 metadata kv
      const blob = new Blob([buf]);
      const result = await (component as any).parseGgufHeader(blob);
      expect(result?.version).toBe(3);
      expect(result?.tensorCount).toBe(722);
      expect(result?.metadataKvCount).toBe(16);
    });
  });
});
