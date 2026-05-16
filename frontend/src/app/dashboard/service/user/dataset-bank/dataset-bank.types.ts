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

export type BankDatasetSource = "dknet" | "uci" | "kaggle";

export type BankCategory =
  | "biomedical"
  | "nlp"
  | "cv"
  | "finance"
  | "social_science"
  | "time_series"
  | "tabular";

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
}

export const BANK_CATEGORY_LABELS: Record<BankCategory, string> = {
  biomedical: "Biomedical",
  nlp: "NLP / Text",
  cv: "Computer Vision",
  finance: "Finance",
  social_science: "Social Science",
  time_series: "Time Series",
  tabular: "Tabular",
};

export const BANK_CATEGORY_ORDER: BankCategory[] = [
  "biomedical",
  "nlp",
  "cv",
  "finance",
  "social_science",
  "time_series",
  "tabular",
];

export const BANK_SOURCE_LABELS: Record<BankDatasetSource, string> = {
  dknet: "dkNET",
  uci: "UCI",
  kaggle: "Kaggle",
};

export const BANK_SOURCE_COLORS: Record<BankDatasetSource, string> = {
  // ng-zorro tag colors
  dknet: "purple",
  uci: "blue",
  kaggle: "cyan",
};
