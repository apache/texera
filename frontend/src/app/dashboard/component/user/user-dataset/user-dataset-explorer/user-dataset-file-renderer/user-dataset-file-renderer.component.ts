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

import { ChangeDetectorRef, Component, EventEmitter, Input, OnChanges, OnDestroy, OnInit, Output, SimpleChanges } from "@angular/core";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import * as Papa from "papaparse";
import { ParseResult } from "papaparse";
import { DomSanitizer, SafeResourceUrl, SafeUrl } from "@angular/platform-browser";
import readXlsxFile, { readSheetNames } from "read-excel-file";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { formatSize } from "../../../../../../common/util/size-formatter.util";
import { Router } from "@angular/router";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../../../common/service/workflow-persist/workflow-persist.service";
import { GuiConfigService } from "../../../../../../common/service/gui-config.service";
import { ExecutionMode, WorkflowContent } from "../../../../../../common/type/workflow";
import { DASHBOARD_USER_WORKSPACE } from "../../../../../../app-routing.constant";
import { NgStyle, NgIf, NgFor } from "@angular/common";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import {
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { MarkdownComponent } from "ngx-markdown";
import { NgxJsonViewerModule } from "ngx-json-viewer";
import { fileTypeFromBlob } from "file-type";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";

export const MIME_TYPES = {
  JPEG: "image/jpeg",
  JPG: "image/jpeg",
  PNG: "image/png",
  WEBP: "image/webp",
  GIF: "image/gif",
  AVIF: "image/avif",
  BMP: "image/bmp",
  TIFF: "image/tiff",
  CSV: "text/csv",
  TXT: "text/plain",
  MD: "text/markdown",
  HTML: "text/html",
  JSON: "application/json",
  PDF: "application/pdf",
  MSWORD: "application/msword",
  MSEXCEL: "application/vnd.ms-excel",
  XLSX: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  DOCX: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  PPTX: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  MSPOWERPOINT: "application/vnd.ms-powerpoint",
  MP4: "video/mp4",
  MP3: "audio/mpeg",
  WAV: "audio/wav",
  FLAC: "audio/flac",
  WEBM: "video/webm",
  MOV: "video/quicktime",
  ARROW: "application/x-arrow",
  PARQUET: "application/x-parquet",
  // ML / scientific data formats
  HDF5: "application/x-hdf5",
  H5AD: "application/x-h5ad",
  H5SEURAT: "application/x-h5seurat",
  LOOM: "application/x-loom",
  PICKLE: "application/x-python-pickle",
  NPY: "application/x-numpy-array",
  NPZ: "application/x-numpy-archive",
  SAFETENSORS: "application/x-safetensors",
  GGUF: "application/x-gguf",
  PYTORCH: "application/x-pytorch",
  KERAS: "application/x-keras",
  ONNX: "application/x-onnx",
  RDS: "application/x-rds",
  // Bioinformatics text
  FASTA: "application/x-fasta",
  FASTQ: "application/x-fastq",
  VCF: "application/x-vcf",
  OCTET_STREAM: "application/octet-stream",
};

export function getMimeType(filename: string): string {
  const extensionMap: Record<string, string> = {
    JPG: MIME_TYPES.JPEG,
    JPEG: MIME_TYPES.JPEG,
    PNG: MIME_TYPES.PNG,
    WEBP: MIME_TYPES.WEBP,
    GIF: MIME_TYPES.GIF,
    AVIF: MIME_TYPES.AVIF,
    BMP: MIME_TYPES.BMP,
    TIFF: MIME_TYPES.TIFF,
    TIF: MIME_TYPES.TIFF,
    CSV: MIME_TYPES.CSV,
    TSV: MIME_TYPES.CSV,
    TXT: MIME_TYPES.TXT,
    MD: MIME_TYPES.MD,
    HTML: MIME_TYPES.HTML,
    HTM: MIME_TYPES.HTML,
    JSON: MIME_TYPES.JSON,
    JSONL: MIME_TYPES.TXT,
    PDF: MIME_TYPES.PDF,
    DOC: MIME_TYPES.MSWORD,
    XLS: MIME_TYPES.MSEXCEL,
    XLSX: MIME_TYPES.XLSX,
    DOCX: MIME_TYPES.DOCX,
    PPTX: MIME_TYPES.PPTX,
    PPT: MIME_TYPES.MSPOWERPOINT,
    MP4: MIME_TYPES.MP4,
    MP3: MIME_TYPES.MP3,
    WAV: MIME_TYPES.WAV,
    FLAC: MIME_TYPES.FLAC,
    WEBM: MIME_TYPES.WEBM,
    MOV: MIME_TYPES.MOV,
    ARROW: MIME_TYPES.ARROW,
    FEATHER: MIME_TYPES.ARROW,
    PARQUET: MIME_TYPES.PARQUET,
    // ML / scientific
    H5: MIME_TYPES.HDF5,
    HDF5: MIME_TYPES.HDF5,
    H5AD: MIME_TYPES.H5AD,
    H5SEURAT: MIME_TYPES.H5SEURAT,
    LOOM: MIME_TYPES.LOOM,
    PKL: MIME_TYPES.PICKLE,
    PICKLE: MIME_TYPES.PICKLE,
    JOBLIB: MIME_TYPES.PICKLE,
    NPY: MIME_TYPES.NPY,
    NPZ: MIME_TYPES.NPZ,
    SAFETENSORS: MIME_TYPES.SAFETENSORS,
    GGUF: MIME_TYPES.GGUF,
    PT: MIME_TYPES.PYTORCH,
    PTH: MIME_TYPES.PYTORCH,
    KERAS: MIME_TYPES.KERAS,
    ONNX: MIME_TYPES.ONNX,
    RDS: MIME_TYPES.RDS,
    // Bioinformatics text
    FASTA: MIME_TYPES.FASTA,
    FA: MIME_TYPES.FASTA,
    FNA: MIME_TYPES.FASTA,
    FFN: MIME_TYPES.FASTA,
    FAA: MIME_TYPES.FASTA,
    FASTQ: MIME_TYPES.FASTQ,
    FQ: MIME_TYPES.FASTQ,
    VCF: MIME_TYPES.VCF,
  };
  const ext = filename.split(".").pop()?.toUpperCase() ?? "";
  return extensionMap[ext] ?? MIME_TYPES.OCTET_STREAM;
}

export function formatDuration(seconds: number): string {
  if (!isFinite(seconds) || seconds < 0) return "—";
  const totalSec = Math.floor(seconds);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  return `${m}:${String(s).padStart(2, "0")}`;
}

/**
 * Maximum size at which we'll attempt to preview a file.
 *
 * Note on memory: for "identify-only" types (HDF5, Parquet, Arrow, pickle, model containers, etc.)
 * we only read the first ~16 bytes for magic-byte detection, so 1 GB is safe. For header-parse types
 * (Safetensors, GGUF, NumPy .npy) we only read the first few KB. The cost of bumping all limits to
 * 1 GB is the full-blob download time, since the dataset service streams the entire file.
 *
 * For full-content render types (CSV via Papa.parse, XLSX, JSON, large text) memory cost scales
 * with file size — browsers may slow down or OOM well before 1 GB. The user can choose: the guard
 * no longer blocks; if their browser tab struggles, they can close it.
 */
const MAX_PREVIEW_SIZE = 1024 * 1024 * 1024;

// size limits per MIME type — also used as pre-fetch guard
export const MIME_TYPE_SIZE_LIMITS_MB = {
  [MIME_TYPES.JPEG]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PNG]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.WEBP]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.GIF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.AVIF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.BMP]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.TIFF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.CSV]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.TXT]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.MD]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.JSON]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PDF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.MSEXCEL]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.XLSX]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.DOCX]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PPTX]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.MP4]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.WEBM]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.MOV]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.MP3]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.WAV]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.FLAC]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.ARROW]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PARQUET]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.HDF5]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.H5AD]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.H5SEURAT]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.LOOM]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PICKLE]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.NPY]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.NPZ]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.SAFETENSORS]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.GGUF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.PYTORCH]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.KERAS]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.ONNX]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.RDS]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.FASTA]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.FASTQ]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.VCF]: MAX_PREVIEW_SIZE,
  [MIME_TYPES.OCTET_STREAM]: MAX_PREVIEW_SIZE,
};

export interface FileMetadata {
  fileSize?: number;
  // image
  imageWidth?: number;
  imageHeight?: number;
  // video
  videoDuration?: number;
  videoWidth?: number;
  videoHeight?: number;
  // audio
  audioDuration?: number;
  // tabular
  rowCount?: number;
  columnCount?: number;
  columnNames?: string[];
  sheetCount?: number;
  // json
  jsonTopLevelType?: "object" | "array";
  jsonItemCount?: number;
  jsonPreviewKeys?: string[];
  // text / markdown
  lineCount?: number;
  wordCount?: number;
  charCount?: number;
  headingCount?: number;
  // pdf
  pageCount?: number;
  // ML model / tensor data
  modelFormat?: string; // "PyTorch", "Keras", "ONNX", "Safetensors", "GGUF", "TensorFlow"
  containerFormat?: string; // "HDF5", "ZIP archive", "gzip"
  tensorCount?: number;
  parameterCount?: number;
  sampleTensorNames?: string[];
  // NumPy
  dtype?: string;
  shape?: number[];
  // GGUF
  ggufVersion?: number;
  metadataKvCount?: number;
  // Bioinformatics
  sequenceCount?: number;
  sequenceCountIsExact?: boolean;
  variantCount?: number;
  variantCountIsExact?: boolean;

  // Rich tabular schema (CSV / XLSX)
  columnTypes?: string[]; // inferred type per column: "integer", "double", "boolean", "date", "string"
  nullCounts?: number[]; // count of empty cells per column (in sample)
  sampleValues?: string[]; // first non-null value per column

  // JSON schema
  jsonMaxDepth?: number;
  jsonKeyTypes?: { key: string; type: string }[]; // for object roots
  jsonArrayElementType?: string; // for array roots: uniform type or "mixed"

  // PDF /Info dictionary
  pdfTitle?: string;
  pdfAuthor?: string;
  pdfCreator?: string;
  pdfProducer?: string;
  pdfVersion?: string;
  pdfEncrypted?: boolean;

  // Markdown structure
  codeBlockCount?: number;
  linkCount?: number;
  imageCount?: number;
  listItemCount?: number;

  // Plain text / encoding
  encoding?: string; // "UTF-8 BOM", "UTF-8", "ASCII"
  emptyLineCount?: number;
  avgLineLength?: number;
  maxLineLength?: number;

  // NumPy enhanced
  totalElements?: number;
  byteOrder?: string; // "little-endian", "big-endian"
  fortranOrder?: boolean;

  // Safetensors enhanced
  dtypeBreakdown?: { dtype: string; params: number }[];
  largestTensor?: { name: string; shape: number[]; params: number };
  safetensorsMetadata?: { key: string; value: string }[];

  // GGUF enhanced
  ggufArchitecture?: string;
  ggufQuantization?: string;

  // FASTA enhanced
  totalBases?: number;
  gcContent?: number; // 0..1
  minSequenceLength?: number;
  maxSequenceLength?: number;
  avgSequenceLength?: number;
  isProtein?: boolean;

  // VCF enhanced
  vcfSampleCount?: number;
  vcfChromosomes?: string[];
}

/**
 * Above this size, skip the download entirely and show only extension-based
 * identification + a "how to load" hint. The dominant source of preview lag
 * is the full-blob download from the dataset service.
 */
export const FULL_PREVIEW_MAX_BYTES = 50 * 1024 * 1024; // 50 MB

/**
 * One-line "how to load" or "what is this" message per format.
 * Used both when content was downloaded (in renderByMimeType) and when the
 * download was skipped (in showOversizedFileInfo).
 */
export const TYPE_LOADING_HINTS: Record<string, string> = {
  [MIME_TYPES.PARQUET]: "Parquet file. Use the Parquet File Scan operator in Texera to analyze this data.",
  [MIME_TYPES.ARROW]: "Arrow / Feather file. Use the Arrow File Scan operator in Texera.",
  [MIME_TYPES.HDF5]: "HDF5 binary container (Keras .h5 or scientific dataset). Load with h5py / rhdf5.",
  [MIME_TYPES.H5AD]: "AnnData (.h5ad) — single-cell expression matrix. Load with scanpy.read_h5ad().",
  [MIME_TYPES.H5SEURAT]: "Seurat HDF5 object (.h5seurat). Load with SeuratDisk::LoadH5Seurat() in R.",
  [MIME_TYPES.LOOM]: "Loom (.loom) single-cell expression. Load with loompy / scanpy in Python.",
  [MIME_TYPES.RDS]: "R serialized object (.rds) — Seurat / SCE / fitted model. Load with readRDS() in R.",
  [MIME_TYPES.PICKLE]: "Python pickle — serialized model or dataset. Load with pickle.load() in Python.",
  [MIME_TYPES.PYTORCH]: "PyTorch checkpoint (.pt/.pth). Load with torch.load() in Python.",
  [MIME_TYPES.KERAS]: "Keras v3 model (.keras). Load with tf.keras.models.load_model() in Python.",
  [MIME_TYPES.ONNX]: "ONNX model (.onnx). Load with onnxruntime; inspect at netron.app.",
  [MIME_TYPES.SAFETENSORS]: "Safetensors file. Load with safetensors.torch.load_file() in Python.",
  [MIME_TYPES.GGUF]: "GGUF model (llama.cpp / quantized LLM).",
  [MIME_TYPES.NPY]: "NumPy array (.npy). Load with numpy.load() in Python.",
  [MIME_TYPES.NPZ]: "NumPy archive (.npz) — ZIP of .npy arrays. Load with numpy.load().",
  [MIME_TYPES.CSV]: "CSV file. Use the CSV File Scan operator in Texera.",
  [MIME_TYPES.JSON]: "JSON file. Use the JSONL File Scan operator (or Python UDF for nested objects).",
  [MIME_TYPES.XLSX]: "Excel spreadsheet (.xlsx). Convert to CSV or use a Python UDF with openpyxl.",
  [MIME_TYPES.MSEXCEL]: "Excel spreadsheet (.xls). Convert to CSV or use a Python UDF.",
  [MIME_TYPES.FASTA]: "FASTA sequence file. Parse with Biopython SeqIO.",
  [MIME_TYPES.FASTQ]: "FASTQ reads file. Parse with Biopython SeqIO.",
  [MIME_TYPES.VCF]: "VCF variant file. Parse with pyvcf / cyvcf2.",
};

/** Classify a single cell value into a coarse type label. */
function inferCellType(value: string): string {
  if (value === "" || value == null) return "null";
  if (/^-?\d+$/.test(value)) return "integer";
  if (/^-?\d+\.\d+$/.test(value) || /^-?\d+\.?\d*[eE][-+]?\d+$/.test(value)) return "double";
  if (/^(true|false|True|False|TRUE|FALSE)$/.test(value)) return "boolean";
  if (/^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?$/.test(value)) return "date";
  return "string";
}

/** Infer per-column type, null count, and a sample value from tabular data rows. */
export function inferColumnSchema(
  dataRows: string[][],
  columnCount: number,
  sampleLimit: number = 50
): { types: string[]; nullCounts: number[]; samples: string[] } {
  const types: string[] = [];
  const nullCounts: number[] = [];
  const samples: string[] = [];
  const rowsToScan = Math.min(dataRows.length, sampleLimit);

  for (let c = 0; c < columnCount; c++) {
    const typeCounts: Record<string, number> = {};
    let nullCount = 0;
    let firstNonNull = "";

    for (let r = 0; r < rowsToScan; r++) {
      const raw = dataRows[r][c];
      const val = raw == null ? "" : String(raw).trim();
      const t = inferCellType(val);
      if (t === "null") {
        nullCount++;
      } else {
        if (firstNonNull === "") firstNonNull = val;
        typeCounts[t] = (typeCounts[t] ?? 0) + 1;
      }
    }

    const ranked = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
    types.push(ranked[0]?.[0] ?? "string");
    nullCounts.push(nullCount);
    samples.push(firstNonNull);
  }
  return { types, nullCounts, samples };
}

/** Walk an arbitrary JSON value and compute max nesting depth. */
function jsonMaxDepth(value: unknown, depth = 1): number {
  if (Array.isArray(value)) {
    let max = depth;
    for (const item of value) max = Math.max(max, jsonMaxDepth(item, depth + 1));
    return max;
  }
  if (value !== null && typeof value === "object") {
    let max = depth;
    for (const v of Object.values(value as Record<string, unknown>)) {
      max = Math.max(max, jsonMaxDepth(v, depth + 1));
    }
    return max;
  }
  return depth;
}

/** Describe a JS value's type for human display. */
function jsTypeLabel(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return `array(${value.length})`;
  return typeof value;
}

/** Extract /Info dictionary fields from a PDF's raw text. Heuristic but robust for unencrypted PDFs. */
function extractPdfInfo(rawText: string): {
  title?: string;
  author?: string;
  creator?: string;
  producer?: string;
  version?: string;
  encrypted?: boolean;
} {
  const result: ReturnType<typeof extractPdfInfo> = {};
  const versionMatch = rawText.match(/^%PDF-(\d+\.\d+)/);
  if (versionMatch) result.version = versionMatch[1];
  result.encrypted = /\/Encrypt\b/.test(rawText);

  // Match `/Title (value)` or `/Title <hex>` — only the parenthesized form is reliably plain text
  const fieldRe = (name: string) => new RegExp(`/${name}\\s*\\(([^)\\\\]*(?:\\\\.[^)\\\\]*)*)\\)`);
  const grab = (name: string): string | undefined => {
    const m = rawText.match(fieldRe(name));
    if (!m) return undefined;
    // PDF strings can contain \( \) \\ escapes — unescape minimally
    return m[1].replace(/\\([()\\])/g, "$1").trim() || undefined;
  };
  result.title = grab("Title");
  result.author = grab("Author");
  result.creator = grab("Creator");
  result.producer = grab("Producer");
  return result;
}

/** Compute GC content and sequence-length stats from a FASTA blob's text. */
function summarizeFasta(text: string): {
  sequenceCount: number;
  totalBases: number;
  gcContent: number;
  minLen: number;
  maxLen: number;
  avgLen: number;
  isProtein: boolean;
} {
  // Walk character by character — avoids splitting a multi-MB string into a huge array.
  let inHeader = false;
  let sequenceCount = 0;
  let currentLen = 0;
  let totalBases = 0;
  let gcCount = 0;
  let nonNucleotideCount = 0;
  let minLen = Infinity;
  let maxLen = 0;
  const nucleotideSet = new Set(["A", "C", "G", "T", "U", "N", "a", "c", "g", "t", "u", "n"]);

  const finishSequence = () => {
    if (sequenceCount > 0 && currentLen > 0) {
      if (currentLen < minLen) minLen = currentLen;
      if (currentLen > maxLen) maxLen = currentLen;
    }
    currentLen = 0;
  };

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === "\n") {
      if (inHeader) inHeader = false;
      continue;
    }
    if (inHeader) continue;
    if (ch === ">") {
      finishSequence();
      sequenceCount++;
      inHeader = true;
      continue;
    }
    if (ch === "\r" || ch === " " || ch === "\t") continue;
    currentLen++;
    totalBases++;
    if (ch === "G" || ch === "C" || ch === "g" || ch === "c") gcCount++;
    if (!nucleotideSet.has(ch)) nonNucleotideCount++;
  }
  finishSequence();

  return {
    sequenceCount,
    totalBases,
    gcContent: totalBases > 0 ? gcCount / totalBases : 0,
    minLen: minLen === Infinity ? 0 : minLen,
    maxLen,
    avgLen: sequenceCount > 0 ? totalBases / sequenceCount : 0,
    isProtein: totalBases > 0 && nonNucleotideCount / totalBases > 0.1,
  };
}

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-file-renderer",
  templateUrl: "./user-dataset-file-renderer.component.html",
  styleUrls: ["./user-dataset-file-renderer.component.scss"],
  imports: [
    NgStyle,
    NgIf,
    NzSpinComponent,
    NzAlertComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzTbodyComponent,
    MarkdownComponent,
    NgxJsonViewerModule,
    NzButtonComponent,
    NzIconDirective,
  ],
})
export class UserDatasetFileRendererComponent implements OnInit, OnChanges, OnDestroy {
  private DEFAULT_MAX_SIZE = 1024 * 1024 * 1024; // 1 GB

  // For text-based formats we slice to this size before parsing/rendering.
  // Reading 1 GB as a UTF-16 string in JS would balloon to ~2 GB and likely crash the tab.
  private static readonly PREVIEW_TEXT_BYTES = 10 * 1024 * 1024; // 10 MB

  /** Slice the blob if it exceeds the preview limit, returning the slice + whether truncation occurred. */
  private getPreviewSlice(blob: Blob): { slice: Blob; truncated: boolean } {
    const limit = UserDatasetFileRendererComponent.PREVIEW_TEXT_BYTES;
    if (blob.size <= limit) return { slice: blob, truncated: false };
    return { slice: blob.slice(0, limit), truncated: true };
  }

  /** True when text content shown is from a slice rather than the whole file. */
  public previewTruncated: boolean = false;

  public fileURL: string | undefined;
  public safeFileURL: SafeUrl | undefined;
  public safeResourceFileURL: SafeResourceUrl | undefined;

  // table related control
  public displayCSV: boolean = false;
  public displayXlsx: boolean = false;
  public tableDataHeader: any[] = [];
  public tableContent: any[][] = [];

  // image related control
  public displayImage: boolean = false;

  // markdown control
  public displayMarkdown: boolean = false;

  // json control
  public displayJson: boolean = false;

  // video
  public displayMP4: boolean = false;

  // audio
  public displayMP3: boolean = false;

  // PDF
  public displayPDF: boolean = false;

  // plain text
  public displayPlainText: boolean = false;
  public textContent: string = "";

  // shown for detectable-but-unpreviewable types (Parquet, Arrow, DOCX, PPTX)
  public detectedTypeMessage: string = "";

  public fileMetadata: FileMetadata | undefined = undefined;

  // control flags
  public isLoading: boolean = false;
  public isFileSizeUnloadable = false;
  public isFileLoadingError: boolean = false;
  public isFileTypePreviewUnsupported: boolean = false;

  public currentFile: File | undefined = undefined;

  @Input() isMaximized: boolean = false;
  @Input() did: number | undefined;
  @Input() dvid: number | undefined;
  @Input() filePath: string = "";
  @Input() fileSize?: number;
  @Input() isLogin: boolean = false;

  @Output() loadFile = new EventEmitter<{ file: string; prefix: string }>();

  constructor(
    private datasetService: DatasetService,
    private sanitizer: DomSanitizer,
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef,
    private router: Router,
    private workflowPersistService: WorkflowPersistService,
    private config: GuiConfigService
  ) {}

  /** Always available — every file gives the user something useful when opened in a workflow. */
  get canOpenInWorkflow(): boolean {
    return !!this.filePath;
  }

  /** Suggested operator type for this file (used in the post-create notification). */
  private static getSuggestedOperatorName(filePath: string): string {
    const mime = getMimeType(filePath);
    switch (mime) {
      case MIME_TYPES.CSV: return "CSV File Scan";
      case MIME_TYPES.JSON: return "JSONL File Scan";
      case MIME_TYPES.ARROW: return "Arrow File Scan";
      case MIME_TYPES.PARQUET: return "Parquet File Scan";
      default: return "File Scan";
    }
  }

  /**
   * Creates a new empty workflow and navigates to the editor. We deliberately do NOT
   * pre-populate the workflow with an operator — hand-constructing operator JSON without
   * the operator-metadata schema validation tends to produce workflows the editor can't
   * load. Instead we copy the file path to the clipboard and tell the user which operator
   * to drag in. Same UX outcome, far more reliable.
   */
  onOpenInWorkflow(): void {
    const fileName = this.filePath.split("/").pop() ?? "file";
    const suggestedOp = UserDatasetFileRendererComponent.getSuggestedOperatorName(this.filePath);
    const workflowContent: WorkflowContent = {
      operators: [],
      commentBoxes: [],
      links: [],
      operatorPositions: {},
      settings: {
        dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
        executionMode: this.config.env.defaultExecutionMode ?? ExecutionMode.PIPELINED,
      },
    };
    const workflowName = `Analysis of ${fileName}`;
    this.workflowPersistService
      .createWorkflow(workflowContent, workflowName || DEFAULT_WORKFLOW_NAME)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: created => {
          const wid = created?.workflow?.wid;
          if (wid == null) {
            this.notificationService.error("Workflow created but no ID was returned.");
            return;
          }
          // Best-effort clipboard copy of the dataset file path so the user can paste it
          // straight into the operator's File field. Falls back silently if the API isn't
          // available (insecure context, older browser).
          if (navigator.clipboard?.writeText) {
            navigator.clipboard.writeText(this.filePath).catch(() => undefined);
          }
          this.notificationService.success(
            `Workflow created. Drag a "${suggestedOp}" operator and paste the file path (copied to clipboard).`
          );
          this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]).then(navigated => {
            if (!navigated) {
              this.notificationService.error("Navigation to the workflow editor was blocked.");
            }
          });
        },
        error: () => this.notificationService.error("Failed to create workflow"),
      });
  }

  ngOnInit(): void {
    this.reloadFileContent();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if ((changes.did && changes.dvid) || changes.filePath) {
      this.reloadFileContent();
    }
  }

  ngOnDestroy(): void {
    if (this.fileURL) {
      URL.revokeObjectURL(this.fileURL);
    }
  }

  showImageModal = false;

  toggleImageModal() {
    this.showImageModal = !this.showImageModal;
  }

  reloadFileContent() {
    this.turnOffAllDisplay();

    const extensionMime = getMimeType(this.filePath);

    // Skip the full download for large files. The dataset service streams the entire blob;
    // for a 500 MB file we'd wait 30+ seconds just to read its first 16 magic bytes. Above
    // the threshold, fall back to extension-based identification + a "how to load" hint.
    if (this.fileSize != null && this.fileSize > FULL_PREVIEW_MAX_BYTES) {
      this.showOversizedFileInfo(extensionMime);
      return;
    }

    // Hard upper bound (defensive): even small types shouldn't load anything past this.
    const preCheckLimit = MIME_TYPE_SIZE_LIMITS_MB[extensionMime] ?? this.DEFAULT_MAX_SIZE;
    if (this.fileSize != null && this.fileSize > preCheckLimit) {
      this.onFileSizeNotLoadable();
      return;
    }

    if (!this.did || !this.dvid || !this.filePath) return;

    this.isLoading = true;
    this.datasetService
      .retrieveDatasetVersionSingleFile(this.filePath, this.isLogin)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: async (blob: Blob) => {
          this.isLoading = false;

          const detectedMime = await this.detectMimeType(blob, this.filePath);

          // Post-detection size check against the now-known type limit
          const sizeLimit = MIME_TYPE_SIZE_LIMITS_MB[detectedMime] ?? this.DEFAULT_MAX_SIZE;
          if (blob.size > sizeLimit) {
            this.onFileSizeNotLoadable();
            this.notificationService.warning(`File ${this.filePath} is too large to preview`);
            return;
          }

          // currentFile is built lazily inside the CSV case (the only consumer); avoids an
          // extra in-memory copy of the blob for every other type.
          this.renderByMimeType(blob, detectedMime);
        },
        error: () => this.onFileLoadingError(),
      });
  }

  /**
   * Detects the actual MIME type of a blob using four strategies in order:
   * 1. file-type library (magic bytes, ~100 formats) — refined with extension hints for
   *    ZIP/gzip container formats (PyTorch, Keras, NPZ, RDS).
   * 2. Manual magic bytes for data formats not covered by file-type
   *    (Parquet, Arrow, HDF5, NumPy .npy, GGUF, Python pickle).
   * 3. Extension-based fallback for opaque binary formats with no reliable magic bytes
   *    (Safetensors, ONNX).
   * 4. Text sniffing for JSON, CSV, FASTA, FASTQ, VCF, Markdown, and plain text.
   *
   * Uses FileReader throughout for broad environment compatibility (tests, browsers).
   */
  async detectMimeType(blob: Blob, fileName?: string): Promise<string> {
    const ext = (fileName ?? "").split(".").pop()?.toLowerCase() ?? "";

    // 1. file-type library covers images, video, audio, PDF, Office (ZIP-based), and more.
    if (typeof fileTypeFromBlob === "function") {
      try {
        const result = await fileTypeFromBlob(blob);
        if (result) {
          // Refine generic container types (ZIP, gzip) using extension hints
          if (result.mime === "application/zip") {
            if (ext === "pt" || ext === "pth") return MIME_TYPES.PYTORCH;
            if (ext === "keras") return MIME_TYPES.KERAS;
            if (ext === "npz") return MIME_TYPES.NPZ;
          }
          if (result.mime === "application/gzip" && ext === "rds") return MIME_TYPES.RDS;
          return result.mime;
        }
      } catch (_) {}
    }

    // 2. Manual magic bytes for formats not in file-type's signature list.
    try {
      const header = await this.readBlobBytes(blob.slice(0, 16));

      // Parquet: PAR1 at bytes 0–3
      if (header[0] === 0x50 && header[1] === 0x41 && header[2] === 0x52 && header[3] === 0x31) {
        return MIME_TYPES.PARQUET;
      }
      // Arrow IPC: ARROW1 at bytes 0–5
      if (
        header[0] === 0x41 && header[1] === 0x52 && header[2] === 0x52 &&
        header[3] === 0x4f && header[4] === 0x57 && header[5] === 0x31
      ) {
        return MIME_TYPES.ARROW;
      }
      // HDF5: \x89HDF\r\n\x1a\n at bytes 0–7
      if (
        header[0] === 0x89 && header[1] === 0x48 && header[2] === 0x44 && header[3] === 0x46 &&
        header[4] === 0x0d && header[5] === 0x0a && header[6] === 0x1a && header[7] === 0x0a
      ) {
        // Refine HDF5 sub-types by extension (all use identical magic bytes)
        if (ext === "h5ad") return MIME_TYPES.H5AD;
        if (ext === "h5seurat") return MIME_TYPES.H5SEURAT;
        if (ext === "loom") return MIME_TYPES.LOOM;
        return MIME_TYPES.HDF5;
      }
      // NumPy .npy: \x93NUMPY at bytes 0–5
      if (
        header[0] === 0x93 && header[1] === 0x4e && header[2] === 0x55 &&
        header[3] === 0x4d && header[4] === 0x50 && header[5] === 0x59
      ) {
        return MIME_TYPES.NPY;
      }
      // GGUF: ASCII "GGUF" at bytes 0–3
      if (header[0] === 0x47 && header[1] === 0x47 && header[2] === 0x55 && header[3] === 0x46) {
        return MIME_TYPES.GGUF;
      }
      // Python pickle: \x80 + protocol byte (2..5) + \x95 (FRAME opcode in proto 4+)
      if (header[0] === 0x80 && header[1] >= 0x02 && header[1] <= 0x05) {
        return MIME_TYPES.PICKLE;
      }
    } catch (_) {}

    // 3. Extension-based fallback for opaque binaries lacking reliable magic bytes
    if (ext === "safetensors") return MIME_TYPES.SAFETENSORS;
    if (ext === "onnx") return MIME_TYPES.ONNX;

    // 4. Text sniffing for formats with no fixed magic bytes
    try {
      const sample = await this.readBlobText(blob.slice(0, 4096));
      const trimmed = sample.trimStart();
      const firstLine = trimmed.split("\n")[0] ?? "";

      if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
        return MIME_TYPES.JSON;
      }
      if (trimmed.startsWith("# ") || trimmed.startsWith("## ")) {
        return MIME_TYPES.MD;
      }
      // VCF: header line starts with ##fileformat=VCF
      if (firstLine.startsWith("##fileformat=VCF")) {
        return MIME_TYPES.VCF;
      }
      // FASTA: first non-empty/comment line starts with '>'
      if (firstLine.startsWith(">")) {
        return MIME_TYPES.FASTA;
      }
      // FASTQ: 4-line record pattern — line 1 starts '@', line 3 starts '+'
      const lines = trimmed.split("\n");
      if (lines.length >= 4 && lines[0].startsWith("@") && lines[2].startsWith("+")) {
        return MIME_TYPES.FASTQ;
      }
      // CSV heuristic: first line has at least 3 comma-separated fields
      if (firstLine.split(",").length >= 3) {
        return MIME_TYPES.CSV;
      }
      // Printable ASCII/UTF-8 → plain text
      const bytes = await this.readBlobBytes(blob.slice(0, 512));
      const isPrintable = bytes.every(b => b === 9 || b === 10 || b === 13 || (b >= 32 && b <= 126));
      if (isPrintable) return MIME_TYPES.TXT;
    } catch (_) {}

    return MIME_TYPES.OCTET_STREAM;
  }

  /** Parse a NumPy .npy header. Returns dtype, shape, byte order, and Fortran flag or null on failure. */
  private async parseNpyHeader(
    blob: Blob
  ): Promise<{ dtype?: string; shape?: number[]; byteOrder?: string; fortranOrder?: boolean } | null> {
    try {
      const head = await this.readBlobBytes(blob.slice(0, 4096));
      // bytes 0-5: magic, byte 6: major, byte 7: minor
      const major = head[6];
      // v1.0: uint16 LE header length at bytes 8-9; v2.0+: uint32 LE at bytes 8-11
      const headerLen = major >= 2 ? head[8] | (head[9] << 8) | (head[10] << 16) | (head[11] << 24)
                                   : head[8] | (head[9] << 8);
      const headerStart = major >= 2 ? 12 : 10;
      const headerText = new TextDecoder().decode(head.slice(headerStart, headerStart + headerLen));
      const dtypeMatch = headerText.match(/['"]descr['"]\s*:\s*['"]([^'"]+)['"]/);
      const shapeMatch = headerText.match(/['"]shape['"]\s*:\s*\(([^)]*)\)/);
      const fortranMatch = headerText.match(/['"]fortran_order['"]\s*:\s*(True|False)/);
      const shape = shapeMatch
        ? shapeMatch[1].split(",").map(s => s.trim()).filter(s => s.length > 0).map(Number)
        : undefined;
      const dtype = dtypeMatch?.[1];
      // dtype prefix: '<' = little-endian, '>' = big-endian, '|' = byte order N/A, '=' = native
      let byteOrder: string | undefined;
      if (dtype) {
        if (dtype.startsWith("<")) byteOrder = "little-endian";
        else if (dtype.startsWith(">")) byteOrder = "big-endian";
        else if (dtype.startsWith("|")) byteOrder = "n/a";
      }
      const fortranOrder = fortranMatch ? fortranMatch[1] === "True" : undefined;
      return { dtype, shape, byteOrder, fortranOrder };
    } catch {
      return null;
    }
  }

  /** Parse a Safetensors file header. Returns rich tensor metadata or null. */
  private async parseSafetensorsHeader(blob: Blob): Promise<{
    tensorCount: number;
    parameterCount: number;
    sampleNames: string[];
    dtypeBreakdown: { dtype: string; params: number }[];
    largestTensor?: { name: string; shape: number[]; params: number };
    metadata?: { key: string; value: string }[];
  } | null> {
    try {
      const lenBytes = await this.readBlobBytes(blob.slice(0, 8));
      // uint64 LE — JS can read up to 53 bits safely; header is always small (KB-MB)
      let headerLen = 0;
      for (let i = 0; i < 8; i++) headerLen += lenBytes[i] * Math.pow(256, i);
      if (headerLen <= 0 || headerLen > 100 * 1024 * 1024) return null;
      const headerText = await this.readBlobText(blob.slice(8, 8 + headerLen));
      const json = JSON.parse(headerText);
      const names = Object.keys(json).filter(k => k !== "__metadata__");
      let paramCount = 0;
      const dtypeMap: Record<string, number> = {};
      let largest: { name: string; shape: number[]; params: number } | undefined;
      for (const name of names) {
        const shape: number[] = json[name]?.shape ?? [];
        const dtype: string = json[name]?.dtype ?? "?";
        const params = shape.length > 0 ? shape.reduce((a, b) => a * b, 1) : 0;
        paramCount += params;
        dtypeMap[dtype] = (dtypeMap[dtype] ?? 0) + params;
        if (!largest || params > largest.params) largest = { name, shape, params };
      }
      const dtypeBreakdown = Object.entries(dtypeMap)
        .sort((a, b) => b[1] - a[1])
        .map(([dtype, params]) => ({ dtype, params }));
      const meta = (json.__metadata__ ?? {}) as Record<string, string>;
      const metadata = Object.entries(meta)
        .slice(0, 6)
        .map(([key, value]) => ({ key, value: String(value) }));
      return {
        tensorCount: names.length,
        parameterCount: paramCount,
        sampleNames: names.slice(0, 5),
        dtypeBreakdown,
        largestTensor: largest,
        metadata: metadata.length > 0 ? metadata : undefined,
      };
    } catch {
      return null;
    }
  }

  /** Parse a GGUF (llama.cpp model) header. Returns version/tensor count or null. */
  private async parseGgufHeader(
    blob: Blob
  ): Promise<{ version: number; tensorCount: number; metadataKvCount: number } | null> {
    try {
      const head = await this.readBlobBytes(blob.slice(0, 24));
      // bytes 0-3: "GGUF" magic
      // bytes 4-7: version (uint32 LE)
      const version = head[4] | (head[5] << 8) | (head[6] << 16) | (head[7] << 24);
      // bytes 8-15: tensor count (uint64 LE)
      let tensorCount = 0;
      for (let i = 0; i < 8; i++) tensorCount += head[8 + i] * Math.pow(256, i);
      // bytes 16-23: metadata kv count (uint64 LE)
      let metadataKvCount = 0;
      for (let i = 0; i < 8; i++) metadataKvCount += head[16 + i] * Math.pow(256, i);
      return { version, tensorCount, metadataKvCount };
    } catch {
      return null;
    }
  }

  private readBlobBytes(blob: Blob): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
      reader.onerror = () => reject(reader.error);
      reader.readAsArrayBuffer(blob);
    });
  }

  private readBlobText(blob: Blob): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = () => reject(reader.error);
      reader.readAsText(blob);
    });
  }

  /**
   * Returns true for any MIME type we know how to render or describe.
   * Only truly unidentified binary (OCTET_STREAM) is considered unsupported.
   */
  isPreviewSupported(mimeType: string): boolean {
    return mimeType !== MIME_TYPES.OCTET_STREAM;
  }

  get metadataItems(): { label: string; value: string }[] {
    const m = this.fileMetadata;
    if (!m) return [];
    const items: { label: string; value: string }[] = [];

    if (m.imageWidth != null && m.imageHeight != null) {
      items.push({ label: "Dimensions", value: `${m.imageWidth} × ${m.imageHeight} px` });
      const gcd = (a: number, b: number): number => (b === 0 ? a : gcd(b, a % b));
      const g = gcd(m.imageWidth, m.imageHeight);
      items.push({ label: "Aspect ratio", value: `${m.imageWidth / g}:${m.imageHeight / g}` });
    }

    if (m.videoDuration != null) items.push({ label: "Duration", value: formatDuration(m.videoDuration) });
    if (m.videoWidth != null && m.videoHeight != null)
      items.push({ label: "Resolution", value: `${m.videoWidth} × ${m.videoHeight}` });

    if (m.audioDuration != null) items.push({ label: "Duration", value: formatDuration(m.audioDuration) });

    if (m.rowCount != null) items.push({ label: "Rows", value: m.rowCount.toLocaleString() });
    if (m.columnCount != null) items.push({ label: "Columns", value: m.columnCount.toLocaleString() });
    if (m.sheetCount != null) items.push({ label: "Sheets", value: m.sheetCount.toLocaleString() });
    if (m.columnNames?.length) {
      const preview = m.columnNames.slice(0, 8).join(", ");
      const more = m.columnNames.length > 8 ? ` +${m.columnNames.length - 8} more` : "";
      items.push({ label: "Fields", value: preview + more });
    }

    if (m.jsonTopLevelType != null) {
      const label = m.jsonTopLevelType === "array" ? "Items" : "Keys";
      items.push({ label: "JSON", value: m.jsonTopLevelType });
      if (m.jsonItemCount != null) items.push({ label, value: m.jsonItemCount.toLocaleString() });
      if (m.jsonPreviewKeys?.length) items.push({ label: "Preview", value: m.jsonPreviewKeys.join(", ") });
    }

    if (m.lineCount != null) items.push({ label: "Lines", value: m.lineCount.toLocaleString() });
    if (m.wordCount != null) items.push({ label: "Words", value: m.wordCount.toLocaleString() });
    if (m.charCount != null) items.push({ label: "Characters", value: m.charCount.toLocaleString() });
    if (m.headingCount != null) items.push({ label: "Headings", value: m.headingCount.toLocaleString() });

    if (m.pageCount != null) items.push({ label: "Pages", value: `~${m.pageCount}` });

    // ML / scientific
    if (m.modelFormat) items.push({ label: "Format", value: m.modelFormat });
    if (m.containerFormat) items.push({ label: "Container", value: m.containerFormat });
    if (m.dtype) items.push({ label: "dtype", value: m.dtype });
    if (m.shape?.length) items.push({ label: "Shape", value: `(${m.shape.join(", ")})` });
    if (m.tensorCount != null) items.push({ label: "Tensors", value: m.tensorCount.toLocaleString() });
    if (m.parameterCount != null) items.push({ label: "Parameters", value: `~${m.parameterCount.toLocaleString()}` });
    if (m.sampleTensorNames?.length)
      items.push({ label: "Tensors (first)", value: m.sampleTensorNames.join(", ") });
    if (m.ggufVersion != null) items.push({ label: "GGUF version", value: `v${m.ggufVersion}` });
    if (m.metadataKvCount != null) items.push({ label: "Metadata KV", value: m.metadataKvCount.toLocaleString() });

    // JSON schema details
    if (m.jsonMaxDepth != null) items.push({ label: "Max depth", value: m.jsonMaxDepth.toLocaleString() });
    if (m.jsonArrayElementType) items.push({ label: "Element type", value: m.jsonArrayElementType });
    if (m.jsonKeyTypes?.length) {
      items.push({
        label: "Schema",
        value: m.jsonKeyTypes.map(kt => `${kt.key}: ${kt.type}`).join(", "),
      });
    }

    // PDF /Info
    if (m.pdfVersion) items.push({ label: "PDF version", value: m.pdfVersion });
    if (m.pdfTitle) items.push({ label: "Title", value: m.pdfTitle });
    if (m.pdfAuthor) items.push({ label: "Author", value: m.pdfAuthor });
    if (m.pdfCreator) items.push({ label: "Creator", value: m.pdfCreator });
    if (m.pdfProducer) items.push({ label: "Producer", value: m.pdfProducer });
    if (m.pdfEncrypted) items.push({ label: "Encrypted", value: "Yes" });

    // Markdown structure
    if (m.codeBlockCount) items.push({ label: "Code blocks", value: m.codeBlockCount.toLocaleString() });
    if (m.linkCount) items.push({ label: "Links", value: m.linkCount.toLocaleString() });
    if (m.imageCount) items.push({ label: "Images", value: m.imageCount.toLocaleString() });
    if (m.listItemCount) items.push({ label: "List items", value: m.listItemCount.toLocaleString() });

    // Plain text encoding/structure
    if (m.encoding) items.push({ label: "Encoding", value: m.encoding });
    if (m.emptyLineCount != null && m.emptyLineCount > 0)
      items.push({ label: "Blank lines", value: m.emptyLineCount.toLocaleString() });
    if (m.avgLineLength != null && m.avgLineLength > 0)
      items.push({ label: "Avg line", value: `${Math.round(m.avgLineLength)} chars` });
    if (m.maxLineLength != null && m.maxLineLength > 0)
      items.push({ label: "Max line", value: `${m.maxLineLength.toLocaleString()} chars` });

    // NumPy details
    if (m.totalElements != null) items.push({ label: "Elements", value: m.totalElements.toLocaleString() });
    if (m.byteOrder) items.push({ label: "Byte order", value: m.byteOrder });
    if (m.fortranOrder != null) items.push({ label: "Order", value: m.fortranOrder ? "Fortran (column)" : "C (row)" });

    // Safetensors details
    if (m.dtypeBreakdown?.length) {
      items.push({
        label: "Dtypes",
        value: m.dtypeBreakdown.map(d => `${d.dtype}: ${d.params.toLocaleString()}`).join(", "),
      });
    }
    if (m.largestTensor) {
      items.push({
        label: "Largest tensor",
        value: `${m.largestTensor.name} (${m.largestTensor.shape.join("×")}, ${m.largestTensor.params.toLocaleString()} params)`,
      });
    }
    if (m.safetensorsMetadata?.length) {
      for (const kv of m.safetensorsMetadata) {
        items.push({ label: kv.key, value: kv.value });
      }
    }

    // GGUF details
    if (m.ggufArchitecture) items.push({ label: "Architecture", value: m.ggufArchitecture });
    if (m.ggufQuantization) items.push({ label: "Quantization", value: m.ggufQuantization });

    // Bioinformatics
    if (m.sequenceCount != null) {
      const label = m.sequenceCountIsExact ? "Sequences" : "Sequences (sampled)";
      items.push({ label, value: m.sequenceCount.toLocaleString() });
    }
    if (m.variantCount != null) {
      const label = m.variantCountIsExact ? "Variants" : "Variants (sampled)";
      items.push({ label, value: m.variantCount.toLocaleString() });
    }
    if (m.totalBases != null) items.push({ label: "Total bases", value: m.totalBases.toLocaleString() });
    if (m.gcContent != null) items.push({ label: "GC content", value: `${(m.gcContent * 100).toFixed(1)}%` });
    if (m.isProtein) items.push({ label: "Sequence type", value: "Protein" });
    if (m.minSequenceLength != null && m.maxSequenceLength != null) {
      items.push({
        label: "Length range",
        value: `${m.minSequenceLength.toLocaleString()}–${m.maxSequenceLength.toLocaleString()} (avg ${Math.round(
          m.avgSequenceLength ?? 0
        ).toLocaleString()})`,
      });
    }
    if (m.vcfSampleCount != null && m.vcfSampleCount > 0)
      items.push({ label: "Samples", value: m.vcfSampleCount.toLocaleString() });
    if (m.vcfChromosomes?.length)
      items.push({ label: "Chromosomes", value: m.vcfChromosomes.slice(0, 8).join(", ") });

    return items;
  }

  private renderByMimeType(blob: Blob, mimeType: string): void {
    if (mimeType.startsWith("image/")) {
      this.displayImage = true;
      this.loadSafeURL(blob);
      this.fileMetadata = { fileSize: blob.size };
      const img = new Image();
      img.onload = () => {
        this.fileMetadata = { ...this.fileMetadata, imageWidth: img.naturalWidth, imageHeight: img.naturalHeight };
        this.cdr.markForCheck();
      };
      img.src = this.fileURL!;
      return;
    }

    if (mimeType.startsWith("video/")) {
      this.displayMP4 = true;
      this.loadSafeURL(blob);
      this.fileMetadata = { fileSize: blob.size };
      const video = document.createElement("video");
      video.preload = "metadata";
      video.onloadedmetadata = () => {
        this.fileMetadata = {
          ...this.fileMetadata,
          videoDuration: video.duration,
          videoWidth: video.videoWidth,
          videoHeight: video.videoHeight,
        };
        this.cdr.markForCheck();
        URL.revokeObjectURL(video.src);
      };
      video.src = URL.createObjectURL(blob);
      return;
    }

    if (mimeType.startsWith("audio/")) {
      this.displayMP3 = true;
      this.loadSafeURL(blob);
      this.fileMetadata = { fileSize: blob.size };
      const audio = document.createElement("audio");
      audio.preload = "metadata";
      audio.onloadedmetadata = () => {
        this.fileMetadata = { ...this.fileMetadata, audioDuration: audio.duration };
        this.cdr.markForCheck();
        URL.revokeObjectURL(audio.src);
      };
      audio.src = URL.createObjectURL(blob);
      return;
    }

    switch (mimeType) {
      case MIME_TYPES.PDF:
        this.displayPDF = true;
        this.loadSafeURL(blob);
        this.fileMetadata = { fileSize: blob.size };
        // Read first 200KB for /Info + version + page count; tail 50KB for trailer (where /Info often lives)
        Promise.all([
          this.readBlobText(blob.slice(0, 200 * 1024)),
          this.readBlobText(blob.slice(Math.max(0, blob.size - 50 * 1024))),
        ]).then(([head, tail]) => {
          const combined = head + "\n" + tail;
          const exact = (combined.match(/\/Type\s*\/Page\b/g) ?? []).length;
          const fallback = Math.ceil((combined.match(/\/Page\b/g) ?? []).length / 2);
          const pageCount = exact > 0 ? exact : fallback || undefined;
          const info = extractPdfInfo(combined);
          this.fileMetadata = {
            ...this.fileMetadata,
            pageCount,
            pdfTitle: info.title,
            pdfAuthor: info.author,
            pdfCreator: info.creator,
            pdfProducer: info.producer,
            pdfVersion: info.version,
            pdfEncrypted: info.encrypted,
          };
          this.cdr.markForCheck();
        });
        break;

      case MIME_TYPES.MSEXCEL:
      case MIME_TYPES.XLSX:
        Promise.all([readXlsxFile(blob), readSheetNames(blob)]).then(([rows, sheetNames]) => {
          const parsedData = rows.map(row => row.map(cell => (cell != null ? cell.toString() : "")));
          if (parsedData.length > 0) {
            this.loadTabularFile(parsedData);
            this.displayXlsx = true;
            const header = parsedData[0];
            const dataRows = parsedData.slice(1).filter(r => r.some(c => c !== ""));
            const schema = inferColumnSchema(dataRows, header.length);
            this.fileMetadata = {
              fileSize: blob.size,
              rowCount: dataRows.length,
              columnCount: header.length,
              columnNames: header,
              sheetCount: sheetNames.length,
              columnTypes: schema.types,
              nullCounts: schema.nullCounts,
              sampleValues: schema.samples,
            };
            this.cdr.markForCheck();
          }
        });
        break;

      case MIME_TYPES.CSV: {
        this.displayCSV = true;
        const { slice: csvSlice, truncated: csvTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = csvTruncated;
        // Papa.parse needs a File-like; build it from the slice only — no need to keep the full blob.
        const fileToParse = new File([csvSlice], this.filePath, { type: MIME_TYPES.CSV });
        Papa.parse(fileToParse, {
          complete: (results: ParseResult<any>) => {
            if (results.data.length > 0) {
              this.loadTabularFile(results.data);
              const header: string[] = results.data[0].map(String);
              const dataRows = (results.data.slice(1) as any[][])
                .filter(r => r.some((c: any) => c !== ""))
                .map(r => r.map((c: any) => (c == null ? "" : String(c))));
              const schema = inferColumnSchema(dataRows, header.length);
              this.fileMetadata = {
                fileSize: blob.size,
                rowCount: dataRows.length,
                columnCount: header.length,
                columnNames: header,
                columnTypes: schema.types,
                nullCounts: schema.nullCounts,
                sampleValues: schema.samples,
              };
              this.cdr.markForCheck();
            }
          },
          error: () => this.onFileLoadingError(),
        });
        break;
      }

      case MIME_TYPES.MD: {
        this.displayMarkdown = true;
        const { slice: mdSlice, truncated: mdTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = mdTruncated;
        this.readBlobText(mdSlice).then(text => {
          this.textContent = text;
          const lines = text.split("\n");
          // Strip fenced code blocks to count them; also count inline elements
          const codeBlockCount = (text.match(/^```/gm) ?? []).length / 2;
          const linkCount = (text.match(/\[[^\]]+\]\([^)]+\)/g) ?? []).length;
          const imageCount = (text.match(/!\[[^\]]*\]\([^)]+\)/g) ?? []).length;
          const listItemCount = lines.filter(l => /^\s*[-*+]\s/.test(l) || /^\s*\d+\.\s/.test(l)).length;
          this.fileMetadata = {
            fileSize: blob.size,
            lineCount: lines.length,
            wordCount: text.trim() ? text.trim().split(/\s+/).length : 0,
            headingCount: lines.filter(l => /^#{1,6}\s/.test(l)).length,
            codeBlockCount: Math.floor(codeBlockCount),
            linkCount: linkCount - imageCount, // image syntax is link syntax + leading '!'
            imageCount,
            listItemCount,
          };
          this.cdr.markForCheck();
        });
        break;
      }

      case MIME_TYPES.JSON: {
        this.displayJson = true;
        const { slice: jsonSlice, truncated: jsonTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = jsonTruncated;
        this.readBlobText(jsonSlice).then(text => {
          this.textContent = text;
          try {
            const parsed = JSON.parse(text);
            const isArray = Array.isArray(parsed);
            const keys = isArray ? null : Object.keys(parsed);
            const maxDepth = jsonMaxDepth(parsed);
            let jsonKeyTypes: { key: string; type: string }[] | undefined;
            let jsonArrayElementType: string | undefined;
            if (isArray && parsed.length > 0) {
              const elementTypes = new Set(parsed.slice(0, 20).map(jsTypeLabel));
              jsonArrayElementType = elementTypes.size === 1 ? [...elementTypes][0] : "mixed";
            } else if (!isArray && keys) {
              jsonKeyTypes = keys.slice(0, 8).map(k => ({
                key: k,
                type: jsTypeLabel((parsed as Record<string, unknown>)[k]),
              }));
            }
            this.fileMetadata = {
              fileSize: blob.size,
              jsonTopLevelType: isArray ? "array" : "object",
              jsonItemCount: isArray ? parsed.length : keys!.length,
              jsonPreviewKeys: isArray
                ? parsed.slice(0, 5).map((_: unknown, i: number) => `[${i}]`)
                : keys!.slice(0, 8),
              jsonMaxDepth: maxDepth,
              jsonKeyTypes,
              jsonArrayElementType,
            };
          } catch {
            // Truncated JSON or invalid — fall back to raw text view
            this.fileMetadata = { fileSize: blob.size };
          }
          this.cdr.markForCheck();
        });
        break;
      }

      case MIME_TYPES.PARQUET:
        this.detectedTypeMessage =
          "Parquet file detected. Use the Parquet File Scan operator in Texera to analyze this data.";
        this.fileMetadata = { fileSize: blob.size };
        break;

      case MIME_TYPES.ARROW:
        this.detectedTypeMessage =
          "Arrow/Feather file detected. Use the Arrow File Scan operator in Texera to analyze this data.";
        this.fileMetadata = { fileSize: blob.size };
        break;

      case MIME_TYPES.DOCX:
        this.detectedTypeMessage = "Word document (.docx) detected. Rich document preview is not yet supported.";
        this.fileMetadata = { fileSize: blob.size };
        break;

      case MIME_TYPES.PPTX:
        this.detectedTypeMessage = "PowerPoint (.pptx) detected. Presentation preview is not yet supported.";
        this.fileMetadata = { fileSize: blob.size };
        break;

      // --- ML / scientific data formats ---

      case MIME_TYPES.HDF5:
        this.detectedTypeMessage =
          "HDF5 binary container detected. Likely a model (Keras .h5) or scientific dataset. Load with h5py / rhdf5.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "HDF5" };
        break;

      case MIME_TYPES.H5AD:
        this.detectedTypeMessage =
          "AnnData (.h5ad) detected — single-cell expression matrix in HDF5. Load with scanpy.read_h5ad() in Python.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "HDF5" };
        break;

      case MIME_TYPES.H5SEURAT:
        this.detectedTypeMessage =
          "Seurat HDF5 object (.h5seurat) detected. Load with SeuratDisk::LoadH5Seurat() in R.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "HDF5" };
        break;

      case MIME_TYPES.LOOM:
        this.detectedTypeMessage =
          "Loom (.loom) detected — single-cell expression in HDF5. Load with loompy / scanpy in Python.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "HDF5" };
        break;

      case MIME_TYPES.RDS:
        this.detectedTypeMessage =
          "R serialized object (.rds) detected — commonly a Seurat / SingleCellExperiment / fitted model. Load with readRDS() in R.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "gzip" };
        break;

      case MIME_TYPES.PICKLE:
        this.detectedTypeMessage =
          "Python pickle detected — typically a serialized model (sklearn / joblib) or dataset. Load with pickle.load() in Python.";
        this.fileMetadata = { fileSize: blob.size };
        break;

      case MIME_TYPES.PYTORCH:
        this.detectedTypeMessage =
          "PyTorch checkpoint (.pt/.pth) detected. Load with torch.load() in Python.";
        this.fileMetadata = { fileSize: blob.size, modelFormat: "PyTorch", containerFormat: "ZIP archive" };
        break;

      case MIME_TYPES.KERAS:
        this.detectedTypeMessage =
          "Keras v3 model (.keras) detected. Load with tf.keras.models.load_model() in Python.";
        this.fileMetadata = { fileSize: blob.size, modelFormat: "Keras", containerFormat: "ZIP archive" };
        break;

      case MIME_TYPES.ONNX:
        this.detectedTypeMessage =
          "ONNX model (.onnx) detected — portable neural network. Load with onnxruntime or netron.app for inspection.";
        this.fileMetadata = { fileSize: blob.size, modelFormat: "ONNX" };
        break;

      case MIME_TYPES.NPY:
        this.parseNpyHeader(blob).then(info => {
          const shapeStr = info?.shape ? info.shape.join(" × ") : "?";
          const totalElements = info?.shape?.reduce((a, b) => a * b, 1);
          this.detectedTypeMessage = `NumPy array (.npy) detected — ${info?.dtype ?? "?"} array of shape (${shapeStr}).`;
          this.fileMetadata = {
            fileSize: blob.size,
            dtype: info?.dtype,
            shape: info?.shape,
            totalElements,
            byteOrder: info?.byteOrder,
            fortranOrder: info?.fortranOrder,
          };
          this.cdr.markForCheck();
        });
        break;

      case MIME_TYPES.NPZ:
        this.detectedTypeMessage =
          "NumPy archive (.npz) detected — ZIP of .npy arrays. Load with numpy.load() and access via dict-like API.";
        this.fileMetadata = { fileSize: blob.size, containerFormat: "ZIP archive" };
        break;

      case MIME_TYPES.SAFETENSORS:
        this.parseSafetensorsHeader(blob).then(info => {
          if (info) {
            const paramStr = info.parameterCount.toLocaleString();
            this.detectedTypeMessage = `Safetensors model detected — ${info.tensorCount} tensors, ~${paramStr} parameters.`;
            this.fileMetadata = {
              fileSize: blob.size,
              modelFormat: "Safetensors",
              tensorCount: info.tensorCount,
              parameterCount: info.parameterCount,
              sampleTensorNames: info.sampleNames,
              dtypeBreakdown: info.dtypeBreakdown,
              largestTensor: info.largestTensor,
              safetensorsMetadata: info.metadata,
            };
          } else {
            this.detectedTypeMessage = "Safetensors file detected. Load with safetensors.torch.load_file() in Python.";
            this.fileMetadata = { fileSize: blob.size, modelFormat: "Safetensors" };
          }
          this.cdr.markForCheck();
        });
        break;

      case MIME_TYPES.GGUF:
        this.parseGgufHeader(blob).then(info => {
          if (info) {
            this.detectedTypeMessage = `GGUF model detected — v${info.version}, ${info.tensorCount} tensors, ${info.metadataKvCount} metadata entries.`;
            this.fileMetadata = {
              fileSize: blob.size,
              modelFormat: "GGUF",
              ggufVersion: info.version,
              tensorCount: info.tensorCount,
              metadataKvCount: info.metadataKvCount,
            };
          } else {
            this.detectedTypeMessage = "GGUF model detected (llama.cpp / quantized LLM format).";
            this.fileMetadata = { fileSize: blob.size, modelFormat: "GGUF" };
          }
          this.cdr.markForCheck();
        });
        break;

      // --- Bioinformatics text formats — render as plain text plus record-count metadata ---

      case MIME_TYPES.FASTA: {
        this.displayPlainText = true;
        const { slice: faSlice, truncated: faTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = faTruncated;
        this.readBlobText(faSlice).then(text => {
          this.textContent = text;
          const stats = summarizeFasta(text);
          this.fileMetadata = {
            fileSize: blob.size,
            lineCount: text.split("\n").length,
            sequenceCount: stats.sequenceCount,
            sequenceCountIsExact: !faTruncated,
            totalBases: stats.totalBases,
            gcContent: stats.isProtein ? undefined : stats.gcContent,
            minSequenceLength: stats.minLen,
            maxSequenceLength: stats.maxLen,
            avgSequenceLength: stats.avgLen,
            isProtein: stats.isProtein,
          };
          this.cdr.markForCheck();
        });
        break;
      }

      case MIME_TYPES.FASTQ: {
        this.displayPlainText = true;
        const { slice: fqSlice, truncated: fqTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = fqTruncated;
        this.readBlobText(fqSlice).then(text => {
          this.textContent = text;
          const lineCount = text.split("\n").filter(l => l.length > 0).length;
          this.fileMetadata = {
            fileSize: blob.size,
            lineCount: text.split("\n").length,
            sequenceCount: Math.floor(lineCount / 4),
            sequenceCountIsExact: !fqTruncated,
          };
          this.cdr.markForCheck();
        });
        break;
      }

      case MIME_TYPES.VCF: {
        this.displayPlainText = true;
        const { slice: vcfSlice, truncated: vcfTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = vcfTruncated;
        this.readBlobText(vcfSlice).then(text => {
          this.textContent = text;
          const lines = text.split("\n");
          const variantLines = lines.filter(l => l.length > 0 && !l.startsWith("#"));
          // Sample names are tab-separated columns after the 9 fixed VCF fields on the #CHROM header line
          const chromHeader = lines.find(l => l.startsWith("#CHROM"));
          const headerFields = chromHeader ? chromHeader.split("\t") : [];
          const vcfSampleCount = headerFields.length > 9 ? headerFields.length - 9 : 0;
          const chromSet = new Set<string>();
          for (const line of variantLines.slice(0, 5000)) {
            const chr = line.split("\t", 1)[0];
            if (chr) chromSet.add(chr);
            if (chromSet.size >= 30) break;
          }
          this.fileMetadata = {
            fileSize: blob.size,
            lineCount: lines.length,
            variantCount: variantLines.length,
            variantCountIsExact: !vcfTruncated,
            vcfSampleCount,
            vcfChromosomes: [...chromSet].slice(0, 12),
          };
          this.cdr.markForCheck();
        });
        break;
      }

      case MIME_TYPES.OCTET_STREAM:
        this.onFileTypePreviewUnsupported();
        break;

      default: {
        this.displayPlainText = true;
        const { slice: txtSlice, truncated: txtTruncated } = this.getPreviewSlice(blob);
        this.previewTruncated = txtTruncated;
        Promise.all([this.readBlobBytes(blob.slice(0, 3)), this.readBlobText(txtSlice)]).then(([head, text]) => {
          this.textContent = text;
          const lines = text.split("\n");
          const lineLens = lines.map(l => l.length);
          const totalLen = lineLens.reduce((a, b) => a + b, 0);
          const emptyLineCount = lineLens.filter(n => n === 0).length;
          const maxLineLength = lineLens.length > 0 ? Math.max(...lineLens) : 0;
          // BOM detection: UTF-8 BOM is EF BB BF; otherwise assume ASCII/UTF-8
          let encoding = "UTF-8";
          if (head[0] === 0xef && head[1] === 0xbb && head[2] === 0xbf) encoding = "UTF-8 BOM";
          else if (lines.every(l => /^[\x00-\x7F]*$/.test(l))) encoding = "ASCII";
          this.fileMetadata = {
            fileSize: blob.size,
            lineCount: lines.length,
            wordCount: text.trim() ? text.trim().split(/\s+/).length : 0,
            charCount: text.length,
            emptyLineCount,
            avgLineLength: lines.length > 0 ? totalLen / lines.length : 0,
            maxLineLength,
            encoding,
          };
          this.cdr.markForCheck();
        });
      }
    }
  }

  turnOffAllDisplay() {
    this.displayCSV = false;
    this.displayXlsx = false;
    this.displayImage = false;
    this.displayPlainText = false;
    this.displayMarkdown = false;
    this.displayJson = false;
    this.displayMP4 = false;
    this.displayMP3 = false;
    this.displayPDF = false;
    this.detectedTypeMessage = "";
    this.fileMetadata = undefined;
    this.isLoading = false;
    this.isFileLoadingError = false;
    this.isFileSizeUnloadable = false;
    this.isFileTypePreviewUnsupported = false;
    if (this.fileURL) {
      URL.revokeObjectURL(this.fileURL);
    }
    this.fileURL = undefined;
    this.safeFileURL = undefined;
    this.safeResourceFileURL = undefined;
    // Clear cached content so memory is reclaimed when switching files; without these,
    // a previously-loaded 10 MB text or 100K-row table would persist on the component.
    this.textContent = "";
    this.tableContent = [];
    this.tableDataHeader = [];
    this.currentFile = undefined;
    this.previewTruncated = false;
  }

  onFileLoadingError() {
    this.turnOffAllDisplay();
    this.isFileLoadingError = true;
  }

  onFileSizeNotLoadable() {
    this.turnOffAllDisplay();
    this.isFileSizeUnloadable = true;
  }

  onFileTypePreviewUnsupported() {
    this.turnOffAllDisplay();
    this.isFileTypePreviewUnsupported = true;
  }

  /**
   * Skip the download for very large files and show only the extension-based type hint.
   * Avoids the multi-second download + memory cost of fetching a multi-hundred-MB blob
   * just to render its first frame / table / iframe.
   */
  private showOversizedFileInfo(extensionMime: string): void {
    const hint = TYPE_LOADING_HINTS[extensionMime];
    const sizeStr = this.fileSize != null ? formatSize(this.fileSize) : "very large";
    this.detectedTypeMessage = hint
      ? `${hint}  (Preview skipped — file is ${sizeStr}.)`
      : `File is ${sizeStr} — full preview skipped to avoid browser lag. Open in a workflow operator to analyze.`;
    this.cdr.markForCheck();
  }

  private loadSafeURL(blob: Blob): void {
    this.fileURL = URL.createObjectURL(blob);
    this.safeFileURL = this.sanitizer.bypassSecurityTrustUrl(this.fileURL);
    this.safeResourceFileURL = this.sanitizer.bypassSecurityTrustResourceUrl(this.fileURL);
  }


  private loadTabularFile(data: any[][]): void {
    if (data.length > 0) {
      this.tableDataHeader = data[0];
      this.tableContent = data
        .slice(1)
        .map(row => {
          while (row.length < this.tableDataHeader.length) row.push("");
          return row;
        })
        .filter(row => row.some(cell => cell !== ""));
    }
  }
}
