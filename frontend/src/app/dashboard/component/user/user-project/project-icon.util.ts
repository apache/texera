/**
 * Lightweight per-project emoji icon overlay stored in localStorage.
 *
 * Why localStorage: the existing `project` DB table has `color` but no `icon`
 * column, and adding a DB migration is out of scope for this hackathon iteration.
 * Icons persist across reloads for the current browser only.
 */

import { localGetObject, localSetObject } from "../../../../common/util/storage";

const PROJECT_ICONS_KEY = "texera-project-icons";
export const DEFAULT_PROJECT_ICON = "📁";

type IconMap = Record<number, string>;

export function getProjectIcon(pid: number): string {
  const map = localGetObject<IconMap>(PROJECT_ICONS_KEY) ?? {};
  return map[pid] || DEFAULT_PROJECT_ICON;
}

export function setProjectIcon(pid: number, icon: string): void {
  const map = localGetObject<IconMap>(PROJECT_ICONS_KEY) ?? {};
  map[pid] = (icon || DEFAULT_PROJECT_ICON).trim();
  localSetObject(PROJECT_ICONS_KEY, map);
}

export function clearProjectIcon(pid: number): void {
  const map = localGetObject<IconMap>(PROJECT_ICONS_KEY) ?? {};
  delete map[pid];
  localSetObject(PROJECT_ICONS_KEY, map);
}
