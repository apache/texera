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

export interface SnippetOperator {
  operatorId: string; // relative ID local to the snippet (regenerated on paste)
  operatorType: string;
  operatorVersion?: string;
  operatorProperties: { [key: string]: any };
  customDisplayName?: string;
  showAdvanced?: boolean;
  position: { x: number; y: number }; // relative to top-left of bounding box
}

export interface SnippetLink {
  fromOperatorId: string;
  fromPortId: string;
  toOperatorId: string;
  toPortId: string;
}

export interface WorkflowSnippet {
  id: string;
  name: string;
  description: string;
  icon: string;
  category: string;
  operators: SnippetOperator[];
  links: SnippetLink[];
  author: string;
  isPublic: boolean;
  createdAt: string;
  updatedAt?: string;
  seeded?: boolean;
}

export const DEFAULT_SNIPPET_CATEGORY = "My Snippets";

export const SNIPPET_ICON_CHOICES: ReadonlyArray<string> = [
  "📦",
  "🧹",
  "🚀",
  "🔬",
  "📊",
  "🛠️",
  "💡",
  "🧠",
  "⚙️",
  "📁",
];
