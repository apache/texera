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

export type BankDatasetSource = "dknet" | "uci" | "kaggle" | "pubmed" | "who";

export type BankCategory =
  | "biomedical"
  | "nlp"
  | "cv"
  | "finance"
  | "social_science"
  | "time_series"
  | "tabular"
  | "public_health";

export interface BankDataset {
  id: string;
  name: string;
  source: BankDatasetSource;
  description: string;
  url: string;
  /** Direct CSV (or other format) download URL when known. Used by the Import action. */
  downloadUrl?: string;
  format?: string;
  rows?: number;
  columns?: number;
  /** Pretty-printed size, e.g. "1.2 MB". */
  sizeLabel?: string;
  tags: string[];
  categories: BankCategory[];
  /**
   * Source-specific opaque identifier used by the backend import proxy to fetch
   * the canonical data on demand (rather than scraping `url`). Examples:
   *   - PubMed: PMID (e.g. "37398152")
   *   - WHO:    GHO indicator code (e.g. "WHOSIS_000001")
   * For UCI/Kaggle/dkNET entries the proxy uses `downloadUrl || url` and ignores this.
   */
  externalId?: string;
}

export const BANK_CATEGORY_LABELS: Record<BankCategory, string> = {
  biomedical: "Biomedical",
  nlp: "NLP / Text",
  cv: "Computer Vision",
  finance: "Finance",
  social_science: "Social Science",
  time_series: "Time Series",
  tabular: "Tabular",
  public_health: "Public Health",
};

export const BANK_CATEGORY_ORDER: BankCategory[] = [
  "biomedical",
  "nlp",
  "cv",
  "finance",
  "social_science",
  "time_series",
  "tabular",
  "public_health",
];

export const BANK_SOURCE_LABELS: Record<BankDatasetSource, string> = {
  dknet: "dkNET",
  uci: "UCI",
  kaggle: "Kaggle",
  pubmed: "PubMed",
  who: "WHO",
};

export const BANK_SOURCE_COLORS: Record<BankDatasetSource, string> = {
  // ng-zorro tag colors
  dknet: "purple",
  uci: "blue",
  kaggle: "cyan",
  pubmed: "green",
  who: "geekblue",
};
