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
 */

export type WorkflowHubCategory = "biomedical" | "nlp" | "cv" | "finance" | "eda" | "education" | "tabular";

export interface WorkflowHubAgentBadge {
  name: string;
  methodology?: string;
}

export interface WorkflowHubEntry {
  id: string;
  workflowId?: number; // backend wid if linked, otherwise undefined (seed entry)
  authorName: string;
  authorAvatarColor: string; // deterministic background for avatar circle
  title: string;
  description: string;
  category: WorkflowHubCategory;
  tags: string[];
  operators: string[]; // operator names that form the DAG chain
  stars: number;
  forks: number;
  views: number;
  featured: boolean;
  publishedAt: string; // ISO date
  agent?: WorkflowHubAgentBadge;
}

export const WORKFLOW_HUB_CATEGORIES: { key: WorkflowHubCategory | "all"; label: string }[] = [
  { key: "all", label: "All" },
  { key: "biomedical", label: "Biomedical" },
  { key: "nlp", label: "NLP" },
  { key: "cv", label: "Computer Vision" },
  { key: "finance", label: "Finance" },
  { key: "eda", label: "EDA" },
  { key: "education", label: "Education" },
  { key: "tabular", label: "Tabular" },
];

export type WorkflowHubSort = "trending" | "stars" | "forks" | "recent";

export const WORKFLOW_HUB_SORTS: { key: WorkflowHubSort; label: string }[] = [
  { key: "trending", label: "Trending" },
  { key: "stars", label: "Most Stars" },
  { key: "forks", label: "Most Forks" },
  { key: "recent", label: "Recent" },
];
