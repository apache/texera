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

import { BetPilotService, TEXERA_WORKFLOW_IDS, texeraWorkflowUrl } from "./bet-pilot.service";

describe("BetPilotService", () => {
  it("should expose workflow links for imported workflow ids", () => {
    expect(texeraWorkflowUrl("wf2_daily")).toBe(`/dashboard/user/workflow/${TEXERA_WORKFLOW_IDS.wf2_daily}`);
  });

  it("should return null for unknown workflow keys at runtime", () => {
    expect(texeraWorkflowUrl("missing" as keyof typeof TEXERA_WORKFLOW_IDS)).toBeNull();
  });

  it("should return consistent daily pick summary data", () => {
    const dailyPicks = new BetPilotService().getDailyPicks();

    expect(dailyPicks.bankroll).toBeGreaterThan(570);
    expect(dailyPicks.stakePerParlay).toBeGreaterThan(0);
    expect(dailyPicks.slips).toHaveLength(2);
    expect(dailyPicks.picks.map(pick => `${pick.player} ${pick.line}`)).toContain("PatMen 31.5");
    expect(dailyPicks.considered.length).toBeGreaterThan(0);
    expect(dailyPicks.considered.every(player => player.reasonShort.length > 0)).toBe(true);
  });

  it("should link scouting rows to their source match pages", () => {
    const report = new BetPilotService().getScoutingReport();

    expect(report.recent).toHaveLength(8);
    expect(report.recent.every(match => match.matchUrl.startsWith("https://www.vlr.gg/"))).toBe(true);
  });

  it("should show a settled bankroll curve above the starting balance", () => {
    const bankroll = new BetPilotService().getBankroll();

    expect(bankroll.totalBalance).toBeGreaterThan(570);
    expect(bankroll.series[bankroll.series.length - 1].value).toBe(bankroll.totalBalance);
    expect(bankroll.changeAbs).toBeCloseTo(bankroll.totalBalance - bankroll.startingBalance, 2);
  });
});
