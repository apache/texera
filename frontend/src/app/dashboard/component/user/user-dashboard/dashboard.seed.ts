/**
 * Initial dashboard shown on first visit — empty, so the user starts from a
 * clean slate and adds widgets from their own workflows.
 */

import { Dashboard } from "./dashboard.types";

export function buildSeedDashboard(_genId: () => string): Dashboard {
  return {
    id: "seed-empty",
    name: "My First Dashboard",
    description: "Click 'Add Widget' to populate this dashboard from a workflow's results.",
    createdAt: Date.now(),
    updatedAt: Date.now(),
    widgets: [],
  };
}
