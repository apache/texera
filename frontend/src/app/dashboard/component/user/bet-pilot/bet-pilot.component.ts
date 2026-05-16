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

import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { RouterModule } from "@angular/router";
import {
  DASHBOARD_USER_BET_PILOT_TODAY,
  DASHBOARD_USER_BET_PILOT_SCOUTING,
  DASHBOARD_USER_BET_PILOT_HEALTH,
  DASHBOARD_USER_BET_PILOT_BANKROLL,
  DASHBOARD_USER_BET_PILOT_CALIBRATION,
  DASHBOARD_USER_BET_PILOT_GLOSSARY,
  DASHBOARD_USER_BET_PILOT_LINES_INPUT,
} from "../../../../app-routing.constant";

@Component({
  selector: "texera-bet-pilot",
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: "./bet-pilot.component.html",
  styleUrls: ["./bet-pilot.component.scss"],
})
export class BetPilotComponent {
  protected readonly LINK_TODAY = DASHBOARD_USER_BET_PILOT_TODAY;
  protected readonly LINK_SCOUTING = DASHBOARD_USER_BET_PILOT_SCOUTING;
  protected readonly LINK_HEALTH = DASHBOARD_USER_BET_PILOT_HEALTH;
  protected readonly LINK_BANKROLL = DASHBOARD_USER_BET_PILOT_BANKROLL;
  protected readonly LINK_CALIBRATION = DASHBOARD_USER_BET_PILOT_CALIBRATION;
  protected readonly LINK_GLOSSARY = DASHBOARD_USER_BET_PILOT_GLOSSARY;
  protected readonly LINK_LINES_INPUT = DASHBOARD_USER_BET_PILOT_LINES_INPUT;
}
