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

import { createContext, useContext, type ReactNode } from "react";
import type { PropertyValidationContext } from "./schema";
import type { PropertyPath } from "./schema";
import type { ResourceCatalog } from "./resource-catalog";
import type { HuggingFaceCatalog } from "./hugging-face";
import type { MediaPreparation } from "./media-upload";
import type { PresetRepository } from "./presets";

export interface OperatorFormContext extends PropertyValidationContext {
  resourceCatalog?: ResourceCatalog;
  selectingFilesFromDatasetsEnabled?: boolean;
  huggingFaceCatalog?: HuggingFaceCatalog;
  mediaPreparation?: MediaPreparation;
  addUdfParameter?: (name: string, attributeType: string) => void | Promise<void>;
  presetRepository?: PresetRepository;
  userPresetEnabled?: boolean;
  fieldPresentation?: {
    authoring?: boolean;
    overrides?: Readonly<Record<string, { displayName?: string; hidden?: boolean }>>;
    showExposure?: boolean;
    exposedPaths?: readonly string[];
    suppressDescriptions?: boolean;
    canHide?: (path: string) => boolean;
    onRename?: (path: string, displayName: string) => void;
    onHiddenChange?: (path: string, hidden: boolean) => void;
    onExposedChange?: (path: string, exposed: boolean) => void;
  };
  onArrayReorder?: (path: PropertyPath, from: number, to: number) => void;
}
const Context = createContext<OperatorFormContext>({});

export function OperatorFormProvider({ value, children }: { value: OperatorFormContext; children: ReactNode }) {
  return <Context.Provider value={value}>{children}</Context.Provider>;
}

export function useOperatorFormContext(): OperatorFormContext {
  return useContext(Context);
}
