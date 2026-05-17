/**
 * Per-project dataset membership stored in localStorage.
 *
 * Why localStorage: there is no `dataset_of_project` junction table in the
 * Texera schema, so for this hackathon iteration we overlay the association
 * in the browser. Schema work can replace this later without touching callers.
 */

import { localGetObject, localSetObject } from "../../../../common/util/storage";

const PROJECT_DATASETS_KEY = "texera-project-datasets";

type Map = Record<number, number[]>;

function load(): Map {
  return localGetObject<Map>(PROJECT_DATASETS_KEY) ?? {};
}

function save(map: Map): void {
  localSetObject(PROJECT_DATASETS_KEY, map);
}

export function getProjectDatasetIds(pid: number): number[] {
  return load()[pid] ?? [];
}

export function setProjectDatasetIds(pid: number, dids: number[]): void {
  const map = load();
  map[pid] = Array.from(new Set(dids));
  save(map);
}

export function addDatasetsToProject(pid: number, didsToAdd: number[]): void {
  const current = getProjectDatasetIds(pid);
  setProjectDatasetIds(pid, [...current, ...didsToAdd]);
}

export function removeDatasetFromProject(pid: number, did: number): void {
  const current = getProjectDatasetIds(pid);
  setProjectDatasetIds(
    pid,
    current.filter(id => id !== did)
  );
}
