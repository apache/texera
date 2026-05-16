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

import { Injectable } from "@angular/core";

// =========================================================================
// Stand-in data contract. Replace each get*() with a real workflow call
// (via HttpClient) once the model is wired up.
// =========================================================================

export interface ConsideredPlayer {
  status: "SKIPPED" | "NEEDS_REVIEW";
  player: string;
  line: number;
  reasonShort: string;
  reasonDetail: string;
}

export interface PickCard {
  player: string;
  line: number;
  direction: "OVER" | "UNDER";
  finalProjection: number;
  projectionStdDev: number;
  gap: number;
  edgeScore: number;
  confidence: number;
  matchLabel: string;
  startTime: string;
  actualKills?: number;
  llmReasoning: string;
}

export interface SlipSummary {
  title: string;
  multiplier: number;
  originalMultiplier?: number;
  stake: number;
  payout: number;
  promoLabel: string;
  time: string;
  location: string;
  picks: PickCard[];
}

export interface DailyPicks {
  date: string;
  matchLabel: string;
  bankroll: number;
  stakePerParlay: number;
  slips: SlipSummary[];
  picks: PickCard[];
  considered: ConsideredPlayer[];
}

export interface RecentMatch {
  date: string;
  matchUrl: string;
  matchLabel: string;
  map: string;
  agent: string;
  kills: number;
  notes: string;
}

export interface MapProb {
  map: string;
  pct: number;
}

export interface ScouteScenario {
  scenario: string;
  map: string;
  prob: number;
  expectedKills: number;
  notes: string;
}

export interface ScoutingReport {
  player: string;
  team: string;
  opponent: string;
  date: string;
  line: number;
  direction: "OVER" | "UNDER";
  finalProjection: number;
  projectionStdDev: number;
  gap: number;
  edgeScore: number;
  confidence: number;
  recent: RecentMatch[];
  onMapSummary: { map: string; avg: number; std: number; n: number }[];
  slot1Probs: MapProb[];
  slot2Probs: MapProb[];
  scenarios: ScouteScenario[];
  ruleProjected: number;
  neuralResidual: number;
  residualReasons: string[];
  llmReasoning: string;
  kellyFraction: number;
}

export interface CalibrationRow {
  bucket: string;
  predicted: number;
  actual: number;
  verdict: "too optimistic" | "slightly optimistic" | "slightly cautious" | "accurate";
  verdictPos: boolean;
}

export interface EdgeSliceRow {
  slice: string;
  n: number;
  clvPp: number;
}

export interface ModelHealth {
  gatePct: number;
  picksEvaluated: number;
  picksRequired: number;
  rollingClvSeries: { x: number; y: number }[];
  calibrationRows: CalibrationRow[];
  edgeSlices: EdgeSliceRow[];
  takeaway: string;
}

export interface BankrollPoint {
  x: number;
  y: number;
  date: string;
  value: number;
}

export interface Bankroll {
  totalBalance: number;
  startingBalance: number;
  changeAbs: number;
  changePct: number;
  rangeLabel: string;
  series: BankrollPoint[];
  settledBets: number;
  wonBets: number;
  lostBets: number;
  hitRate: number;
  modelClaimedHitRate: number;
}

export interface CalibrationVersion {
  version: string;
  refitDate: string;
  active: boolean;
  changed: string;
  brierScore: number | null;
  trend: "baseline" | "better" | "worse" | "n/a";
}

export interface CalibrationLog {
  versions: CalibrationVersion[];
  nextRefit: string;
  resolvedBetsAvailable: number;
  minBetsRequired: number;
  brierTrendDelta: number;
}

// =========================================================================
// Texera workflow ID mapping. Update the right-hand numbers as each
// workflow JSON gets imported (Your Work → Workflows → Import).
// WF-2 (Daily predict + bet pick) was the first verified import.
// =========================================================================
export const TEXERA_WORKFLOW_IDS = {
  wf0_features: 8, // ~/Desktop/valorant_wf0_overview.json
  wf1_training: 9, // ~/Desktop/valorant_wf1_overview.json
  wf2_daily: 5, // valorant_wf2_overview
  wf3_backtest: 10, // ~/Desktop/valorant_wf3_overview.json
  wf4_calibration: 11, // ~/Desktop/valorant_wf4_overview.json
  wf5_clv_monitor: 12, // ~/Desktop/valorant_wf5_overview.json
  wf6_diagnostics: 13, // ~/Desktop/valorant_wf6_overview.json
} as const;

/** Returns the deep link to a Texera workflow by key, or null if not imported yet. */
export function texeraWorkflowUrl(key: keyof typeof TEXERA_WORKFLOW_IDS): string | null {
  const id = TEXERA_WORKFLOW_IDS[key];
  return id > 0 ? `/dashboard/user/workflow/${id}` : null;
}

@Injectable({ providedIn: "root" })
export class BetPilotService {
  getDailyPicks(): DailyPicks {
    const patMen: PickCard = {
      player: "PatMen",
      line: 31.5,
      direction: "OVER",
      finalProjection: 35,
      projectionStdDev: 3.1,
      gap: 3.5,
      edgeScore: 2.9,
      confidence: 0.72,
      matchLabel: "GE vs FS",
      startTime: "May 15 · 3:00 AM",
      actualKills: 35,
      llmReasoning:
        "Higher 31.5 on Maps 1+2 cleared in both captured slips. The model keeps this as a playable line because PatMen's recent map pool puts his two-map kill expectation in the mid-30s.",
    };
    const invy: PickCard = {
      player: "invy",
      line: 27.5,
      direction: "OVER",
      finalProjection: 31.8,
      projectionStdDev: 3.7,
      gap: 4.3,
      edgeScore: 2.6,
      confidence: 0.69,
      matchLabel: "T1 vs PRX",
      startTime: "May 15 · 5:00 AM",
      llmReasoning:
        "Higher 27.5 on Maps 1+2 fits the screenshot's winning slip. The line is low enough that the model only needs normal round volume, not a ceiling game, to clear.",
    };
    const primmie: PickCard = {
      player: "Primmie",
      line: 38.5,
      direction: "OVER",
      finalProjection: 49,
      projectionStdDev: 4.8,
      gap: 10.5,
      edgeScore: 4.4,
      confidence: 0.81,
      matchLabel: "GE vs FS",
      startTime: "May 15 · 3:00 AM",
      actualKills: 49,
      llmReasoning:
        "Higher 38.5 is the strongest captured result: Primmie finished at 49 kills, giving the model a large positive result gap against a difficult but beatable Champions line.",
    };

    return {
      date: "05/15/2026",
      matchLabel: "Champions · Kills on Maps 1+2",
      bankroll: 574.17,
      stakePerParlay: 30,
      slips: [
        {
          title: "2 Champions picks",
          multiplier: 3.49,
          originalMultiplier: 3.08,
          stake: 30,
          payout: 105.2,
          promoLabel: "20% Squad Boost promotion applied",
          time: "05/14/2026 · 08:37 PM",
          location: "CA, UD Fantasy",
          picks: [patMen, invy],
        },
        {
          title: "2 Champions picks",
          multiplier: 2.97,
          stake: 33,
          payout: 97.17,
          promoLabel: "Squad Boost promotion applied",
          time: "05/14/2026 · 01:21 PM",
          location: "CA, UD Fantasy",
          picks: [primmie, patMen],
        },
      ],
      picks: [primmie, patMen, invy],
      considered: [
        {
          status: "SKIPPED",
          player: "stax",
          line: 25.5,
          reasonShort: "Confidence too low (30%) — we need at least 65%.",
          reasonDetail:
            "The model is only 30% confident on this line. Our minimum is 65% — anything lower, the math doesn't justify the risk.",
        },
        {
          status: "NEEDS_REVIEW",
          player: "Primmie",
          line: 38.5,
          reasonShort: "We don't have this player or team in our database yet.",
          reasonDetail:
            "The screenshot OCR found Primmie Higher 38.5. Until the player alias is linked to a known profile, the row is held for review before a live recommendation.",
        },
        {
          status: "SKIPPED",
          player: "Munchkin",
          line: 26.5,
          reasonShort: "Our projection is too close to the line to bet confidently.",
          reasonDetail:
            "We projected 24.6 kills against a line of 26.5. A gap of only 1.9 isn't enough margin to bet with confidence.",
        },
        {
          status: "SKIPPED",
          player: "BuZz",
          line: 32.5,
          reasonShort: "Data looked unreliable — we flagged this and skipped.",
          reasonDetail:
            "The model output 248.4 kills — clearly an outlier. We flag and skip anything outside reasonable bounds rather than trust bad data.",
        },
        {
          status: "NEEDS_REVIEW",
          player: "invy",
          line: 27.5,
          reasonShort: "We don't have this player or team in our database yet.",
          reasonDetail:
            "The screenshot OCR found invy Higher 27.5. The pick is parked until this spelling is linked to the right esports player profile.",
        },
        {
          status: "SKIPPED",
          player: "Meteor",
          line: 29.5,
          reasonShort: "Our projection is too close to the line to bet confidently.",
          reasonDetail:
            "We projected 28.1 kills against a line of 29.5. A gap of only 1.4 isn't enough margin to bet with confidence.",
        },
      ],
    };
  }

  getScoutingReport(): ScoutingReport {
    return {
      player: "Reduxx",
      team: "Sentinels",
      opponent: "Evil Geniuses",
      date: "05/12/2026",
      line: 32.5,
      direction: "OVER",
      finalProjection: 37.8,
      projectionStdDev: 4.2,
      gap: 5.3,
      edgeScore: 3.71,
      confidence: 0.7,
      recent: [
        {
          date: "05/12/2026",
          matchUrl: "https://www.vlr.gg/674838/sentinels-vs-evil-geniuses-esports-world-cup-2026-americas-qualifier-stage-1-ubsf",
          matchLabel: "SEN vs EG",
          map: "Split",
          agent: "Raze",
          kills: 17,
          notes: "EWC qualifier · 9 rounds",
        },
        {
          date: "05/12/2026",
          matchUrl: "https://www.vlr.gg/674838/sentinels-vs-evil-geniuses-esports-world-cup-2026-americas-qualifier-stage-1-ubsf",
          matchLabel: "SEN vs EG",
          map: "Haven",
          agent: "Raze",
          kills: 6,
          notes: "Short map · low round volume",
        },
        {
          date: "05/10/2026",
          matchUrl: "https://www.vlr.gg/645503/sentinels-vs-nrg-vct-2026-americas-stage-1-w5",
          matchLabel: "SEN vs NRG",
          map: "Corrode",
          agent: "Raze",
          kills: 23,
          notes: "High pace · 18 rounds",
        },
        {
          date: "05/10/2026",
          matchUrl: "https://www.vlr.gg/645503/sentinels-vs-nrg-vct-2026-americas-stage-1-w5",
          matchLabel: "SEN vs NRG",
          map: "Haven",
          agent: "Raze",
          kills: 14,
          notes: "Moderate floor · 17 rounds",
        },
        {
          date: "05/02/2026",
          matchUrl: "https://www.vlr.gg/645495/furia-vs-sentinels-vct-2026-americas-stage-1-w4/?view=linear",
          matchLabel: "FURIA vs SEN",
          map: "Lotus",
          agent: "Raze",
          kills: 18,
          notes: "Loss · still cleared role baseline",
        },
        {
          date: "04/26/2026",
          matchUrl: "https://www.vlr.gg/event/2860/vct-2026-americas-stage-1",
          matchLabel: "EG vs SEN",
          map: "Haven",
          agent: "Omen",
          kills: 19,
          notes: "VCT Americas · series context",
        },
        {
          date: "04/19/2026",
          matchUrl: "https://www.vlr.gg/645485/100-thieves-vs-sentinels-vct-2026-americas-stage-1-w2/?tab=overview",
          matchLabel: "100T vs SEN",
          map: "Haven",
          agent: "Omen",
          kills: 26,
          notes: "2-0 W · high KAST",
        },
        {
          date: "04/19/2026",
          matchUrl: "https://www.vlr.gg/645485/100-thieves-vs-sentinels-vct-2026-americas-stage-1-w2/?tab=overview",
          matchLabel: "100T vs SEN",
          map: "Split",
          agent: "Omen",
          kills: 23,
          notes: "Map 2 win · strong conversion",
        },
      ],
      onMapSummary: [
        { map: "Raze maps", avg: 15.6, std: 5.8, n: 5 },
        { map: "Controller maps", avg: 22.7, std: 3.1, n: 3 },
      ],
      slot1Probs: [{ map: "Split", pct: 61 }, { map: "Haven", pct: 24 }, { map: "Other", pct: 15 }],
      slot2Probs: [
        { map: "Corrode", pct: 42 },
        { map: "Lotus", pct: 31 },
        { map: "Haven", pct: 19 },
        { map: "Other", pct: 8 },
      ],
      scenarios: [
        {
          scenario: "Slot 1",
          map: "Split",
          prob: 0.61,
          expectedKills: 17.1,
          notes: "favored opener; role keeps engagement high",
        },
        { scenario: "Slot 2", map: "Corrode", prob: 0.42, expectedKills: 19.2, notes: "recent NRG map showed ceiling" },
        { scenario: "Slot 2", map: "Lotus", prob: 0.31, expectedKills: 15.8, notes: "loss risk but still high contact" },
        { scenario: "Slot 2", map: "Haven", prob: 0.19, expectedKills: 14.7, notes: "lowest-volume branch" },
      ],
      ruleProjected: 36.6,
      neuralResidual: 1.2,
      residualReasons: [
        "Evil Geniuses series data increased expected round count after the OCR line was parsed",
        "Reduxx's controller-map output gives the model a non-duelist fallback path",
        "Recent VLR pages keep the match links auditable from the scouting table",
      ],
      llmReasoning:
        "Strong HIGHER candidate once the screenshot line is converted into structured input. The workflow combines scraped Underdog lines, VLR recent form, map-pool priors, and a neural residual, then Codex/Claude-facing UI copy turns the model output into a scouting report that can be audited through the linked match pages.",
      kellyFraction: 0.034,
    };
  }

  getModelHealth(): ModelHealth {
    return {
      gatePct: 24.7,
      picksEvaluated: 247,
      picksRequired: 1000,
      rollingClvSeries: [
        { x: 0, y: 90 },
        { x: 30, y: 82 },
        { x: 60, y: 78 },
        { x: 90, y: 85 },
        { x: 120, y: 70 },
        { x: 150, y: 62 },
        { x: 180, y: 65 },
        { x: 210, y: 55 },
        { x: 240, y: 52 },
        { x: 270, y: 60 },
        { x: 300, y: 48 },
        { x: 330, y: 54 },
        { x: 360, y: 50 },
        { x: 400, y: 56 },
      ],
      calibrationRows: [
        {
          bucket: "Said 65–70% confident",
          predicted: 0.67,
          actual: 0.61,
          verdict: "too optimistic",
          verdictPos: false,
        },
        {
          bucket: "Said 70–80% confident",
          predicted: 0.75,
          actual: 0.72,
          verdict: "slightly optimistic",
          verdictPos: false,
        },
        {
          bucket: "Said 80–90% confident",
          predicted: 0.85,
          actual: 0.88,
          verdict: "slightly cautious",
          verdictPos: true,
        },
        { bucket: "Said 90%+ confident", predicted: 0.92, actual: 0.94, verdict: "accurate", verdictPos: true },
      ],
      edgeSlices: [
        { slice: "Map: Bind", n: 42, clvPp: 3.1 },
        { slice: "Map: Haven", n: 51, clvPp: 0.4 },
        { slice: "Map: Pearl", n: 33, clvPp: -1.2 },
        { slice: "Role: Duelist", n: 88, clvPp: 2.1 },
        { slice: "Role: Initiator", n: 64, clvPp: 0.6 },
        { slice: "Role: Sentinel", n: 29, clvPp: -1.8 },
      ],
      takeaway:
        "The model is over-confident on close calls and slightly under-confident on its strongest picks. Calibration refit (weekly) corrects for this automatically.",
    };
  }

  getBankroll(): Bankroll {
    const series: BankrollPoint[] = [
      { x: 0, y: 205, date: "Apr 15", value: 500.0 },
      { x: 40, y: 208, date: "Apr 17", value: 499.5 },
      { x: 80, y: 203, date: "Apr 19", value: 501.0 },
      { x: 120, y: 214, date: "Apr 21", value: 497.5 },
      { x: 160, y: 198, date: "Apr 23", value: 503.0 },
      { x: 200, y: 190, date: "Apr 25", value: 506.5 },
      { x: 240, y: 196, date: "Apr 27", value: 504.0 },
      { x: 280, y: 184, date: "Apr 29", value: 509.0 },
      { x: 320, y: 174, date: "May 01", value: 514.5 },
      { x: 360, y: 178, date: "May 03", value: 512.0 },
      { x: 400, y: 160, date: "May 05", value: 522.0 },
      { x: 440, y: 168, date: "May 06", value: 518.5 },
      { x: 480, y: 145, date: "May 07", value: 532.0 },
      { x: 520, y: 152, date: "May 08", value: 528.0 },
      { x: 560, y: 122, date: "May 09", value: 548.0 },
      { x: 600, y: 132, date: "May 10", value: 541.0 },
      { x: 640, y: 98, date: "May 11", value: 559.0 },
      { x: 680, y: 112, date: "May 12", value: 552.5 },
      { x: 720, y: 82, date: "May 13", value: 568.0 },
      { x: 760, y: 62, date: "May 14", value: 580.2 },
      { x: 800, y: 72, date: "May 15", value: 574.17 },
    ];
    return {
      totalBalance: 574.17,
      startingBalance: 500.0,
      changeAbs: 74.17,
      changePct: 14.8,
      rangeLabel: "past 30 days",
      series,
      settledBets: 22,
      wonBets: 15,
      lostBets: 7,
      hitRate: 0.682,
      modelClaimedHitRate: 0.69,
    };
  }

  getCalibrationLog(): CalibrationLog {
    return {
      versions: [
        {
          version: "v3",
          refitDate: "05/12/2026",
          active: true,
          changed: "Pulled back confidence in the 65–70% band by ~5 points.",
          brierScore: 0.184,
          trend: "better",
        },
        {
          version: "v2",
          refitDate: "05/05/2026",
          active: false,
          changed: "Boosted confidence in the 80–90% band (was under-claiming).",
          brierScore: 0.198,
          trend: "better",
        },
        {
          version: "v1",
          refitDate: "04/28/2026",
          active: false,
          changed: "First refit · used 217 resolved bets.",
          brierScore: 0.213,
          trend: "baseline",
        },
        {
          version: "v0",
          refitDate: "04/21/2026",
          active: false,
          changed: "Skipped · fewer than 200 resolved bets available.",
          brierScore: null,
          trend: "n/a",
        },
      ],
      nextRefit: "Sunday 05/19/2026",
      resolvedBetsAvailable: 247,
      minBetsRequired: 200,
      brierTrendDelta: -0.029,
    };
  }
}
