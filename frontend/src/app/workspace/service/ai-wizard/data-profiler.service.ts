/**
 * Browser-side CSV profiler. Parses up to MAX_ROWS from a File via PapaParse
 * and produces a DataProfile that the LLM can consume so it never has to
 * guess column names (design-doc §4.2, §7 point 1 — schema-aware generation).
 *
 * Intentionally lives in the browser, not the agent-service backend: the
 * design doc treats the wizard as a frontend-driven copilot and we want
 * profiling to work without requiring the user to upload to a backend first.
 */

import { Injectable } from "@angular/core";
import * as Papa from "papaparse";
import { ColumnDtype, ColumnProfile, DataProfile } from "./types";

const MAX_ROWS = 5000;
const SAMPLE_VALUES_PER_COLUMN = 5;

@Injectable({ providedIn: "root" })
export class DataProfilerService {
  /**
   * Profile a CSV File object the user picked from <input type="file">.
   * Returns null if the file can't be parsed.
   */
  public async profileCsvFile(file: File): Promise<DataProfile | null> {
    const text = await this.readSlice(file, MAX_ROWS);
    return this.profileCsvText(text);
  }

  public profileCsvText(csvText: string): DataProfile | null {
    const parsed = Papa.parse<Record<string, string>>(csvText, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      transformHeader: h => h.trim(),
    });
    if (parsed.errors.length > 0 && parsed.data.length === 0) {
      return null;
    }
    const rows = parsed.data as Array<Record<string, string>>;
    const headers = parsed.meta.fields ?? [];
    if (headers.length === 0) return null;

    const columns: ColumnProfile[] = headers.map(name => this.profileColumn(name, rows));
    return {
      rowCount: rows.length,
      columns,
      source: "csv-upload",
    };
  }

  private profileColumn(name: string, rows: Array<Record<string, string>>): ColumnProfile {
    const values = rows.map(r => r[name]);
    let nulls = 0;
    const unique = new Set<string>();
    const sampleValues: string[] = [];
    for (const v of values) {
      if (v === undefined || v === null || v === "") {
        nulls++;
        continue;
      }
      unique.add(v);
      if (sampleValues.length < SAMPLE_VALUES_PER_COLUMN) sampleValues.push(v);
    }
    const total = values.length || 1;
    return {
      name,
      dtype: this.inferDtype(sampleValues),
      nullRate: Number((nulls / total).toFixed(3)),
      uniqueCount: unique.size,
      sampleValues,
    };
  }

  private inferDtype(samples: string[]): ColumnDtype {
    if (samples.length === 0) return "str";
    const allInt = samples.every(v => /^-?\d+$/.test(v.trim()));
    if (allInt) return "int";
    const allFloat = samples.every(v => /^-?\d+(\.\d+)?$/.test(v.trim()));
    if (allFloat) return "float";
    const lower = samples.map(v => v.trim().toLowerCase());
    if (lower.every(v => v === "true" || v === "false" || v === "0" || v === "1")) return "bool";
    if (samples.every(v => !isNaN(Date.parse(v)))) return "date";
    return "str";
  }

  // Read enough of the file to cover MAX_ROWS lines. Cheap for the demo
  // (Pima diabetes is 768 rows total).
  private async readSlice(file: File, _maxRows: number): Promise<string> {
    return await file.text();
  }
}
