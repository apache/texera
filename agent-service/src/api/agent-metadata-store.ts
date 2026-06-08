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

import { Kysely, PostgresDialect, sql, type RawBuilder, type Selectable } from "kysely";
import { Pool, type PoolConfig } from "pg";
import { env } from "../config/env";
import type { Agent as AgentTable, DB, JsonValue } from "../db/generated";
import type { AgentPersistedConfig, ReActStep } from "../types/agent";

const EMPTY_AGENT_CONFIG: AgentPersistedConfig = {
  systemPrompt: "",
  tools: [],
  settings: {},
};

export interface AgentMetadata {
  id: string;
  ownerUid: number;
  name: string;
  modelType: string;
  createdAt: Date;
  config: AgentPersistedConfig;
  reactSteps: ReActStep[];
}

export interface AgentMetadataStore {
  createAgent(metadata: AgentMetadata): Promise<void>;
  getAgent(agentId: string): Promise<AgentMetadata | undefined>;
  listAgentsByOwner(ownerUid: number): Promise<AgentMetadata[]>;
  updateAgentConfig(agentId: string, config: AgentPersistedConfig): Promise<void>;
  updateAgentReActSteps(agentId: string, reactSteps: ReActStep[]): Promise<void>;
  deleteAgent(agentId: string): Promise<void>;
}

type AgentRow = Selectable<AgentTable>;

const AGENT_SCHEMA = "texera_db";

function jdbcUrlToPoolConfig(jdbcUrl: string): Pick<PoolConfig, "host" | "port" | "database"> {
  const url = new URL(jdbcUrl.replace(/^jdbc:/, ""));
  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 5432,
    database: url.pathname.replace(/^\//, ""),
  };
}

function createKyselyDatabase(): Kysely<DB> {
  return new Kysely<DB>({
    dialect: new PostgresDialect({
      pool: new Pool({
        ...jdbcUrlToPoolConfig(env.STORAGE_JDBC_URL),
        user: env.STORAGE_JDBC_USERNAME,
        password: env.STORAGE_JDBC_PASSWORD,
        max: 5,
      }),
    }),
  });
}

function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function toJsonb(value: AgentPersistedConfig | ReActStep[]): RawBuilder<JsonValue> {
  return sql<JsonValue>`${JSON.stringify(cloneJson(value))}::jsonb`;
}

function parseJsonValue<T>(value: unknown, fallback: T): T {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return fallback;
    }
  }
  if (value === null || value === undefined) {
    return fallback;
  }
  return value as T;
}

function normalizeConfig(value: unknown): AgentPersistedConfig {
  const parsed = parseJsonValue<Partial<AgentPersistedConfig>>(value, EMPTY_AGENT_CONFIG);
  return {
    systemPrompt: typeof parsed.systemPrompt === "string" ? parsed.systemPrompt : "",
    tools: Array.isArray(parsed.tools) ? parsed.tools : [],
    settings: parsed.settings && typeof parsed.settings === "object" ? parsed.settings : {},
  };
}

function normalizeReactSteps(value: unknown): ReActStep[] {
  const parsed = parseJsonValue<unknown>(value, []);
  return Array.isArray(parsed) ? (parsed as ReActStep[]) : [];
}

function rowToMetadata(row: AgentRow): AgentMetadata {
  return {
    id: row.aid,
    ownerUid: row.uid,
    name: row.name,
    modelType: row.model_type,
    createdAt: row.creation_time instanceof Date ? row.creation_time : new Date(row.creation_time),
    config: normalizeConfig(row.config),
    reactSteps: normalizeReactSteps(row.react_steps),
  };
}

export class PostgresAgentMetadataStore implements AgentMetadataStore {
  private readonly db: Kysely<DB>;

  constructor(db?: Kysely<DB>) {
    this.db = db ?? createKyselyDatabase();
  }

  async createAgent(metadata: AgentMetadata): Promise<void> {
    await this.db
      .withSchema(AGENT_SCHEMA)
      .insertInto("agent")
      .values({
        aid: metadata.id,
        uid: metadata.ownerUid,
        name: metadata.name,
        model_type: metadata.modelType,
        config: toJsonb(metadata.config),
        react_steps: toJsonb(metadata.reactSteps),
        creation_time: metadata.createdAt,
      })
      .execute();
  }

  async getAgent(agentId: string): Promise<AgentMetadata | undefined> {
    const row = await this.db
      .withSchema(AGENT_SCHEMA)
      .selectFrom("agent")
      .selectAll()
      .where("aid", "=", agentId)
      .executeTakeFirst();
    return row ? rowToMetadata(row) : undefined;
  }

  async listAgentsByOwner(ownerUid: number): Promise<AgentMetadata[]> {
    const rows = await this.db
      .withSchema(AGENT_SCHEMA)
      .selectFrom("agent")
      .selectAll()
      .where("uid", "=", ownerUid)
      .orderBy("creation_time", "asc")
      .execute();
    return rows.map(rowToMetadata);
  }

  async updateAgentConfig(agentId: string, config: AgentPersistedConfig): Promise<void> {
    await this.db
      .withSchema(AGENT_SCHEMA)
      .updateTable("agent")
      .set({ config: toJsonb(config) })
      .where("aid", "=", agentId)
      .execute();
  }

  async updateAgentReActSteps(agentId: string, reactSteps: ReActStep[]): Promise<void> {
    await this.db
      .withSchema(AGENT_SCHEMA)
      .updateTable("agent")
      .set({ react_steps: toJsonb(reactSteps) })
      .where("aid", "=", agentId)
      .execute();
  }

  async deleteAgent(agentId: string): Promise<void> {
    await this.db.withSchema(AGENT_SCHEMA).deleteFrom("agent").where("aid", "=", agentId).execute();
  }
}

export class InMemoryAgentMetadataStore implements AgentMetadataStore {
  private readonly agents = new Map<string, AgentMetadata>();

  private cloneMetadata(metadata: AgentMetadata): AgentMetadata {
    return {
      ...metadata,
      createdAt: new Date(metadata.createdAt),
      config: cloneJson(metadata.config),
      reactSteps: cloneJson(metadata.reactSteps),
    };
  }

  async createAgent(metadata: AgentMetadata): Promise<void> {
    this.agents.set(metadata.id, this.cloneMetadata(metadata));
  }

  async getAgent(agentId: string): Promise<AgentMetadata | undefined> {
    const metadata = this.agents.get(agentId);
    return metadata ? this.cloneMetadata(metadata) : undefined;
  }

  async listAgentsByOwner(ownerUid: number): Promise<AgentMetadata[]> {
    return Array.from(this.agents.values())
      .filter(agent => agent.ownerUid === ownerUid)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      .map(agent => this.cloneMetadata(agent));
  }

  async updateAgentConfig(agentId: string, config: AgentPersistedConfig): Promise<void> {
    const metadata = this.agents.get(agentId);
    if (metadata) {
      metadata.config = cloneJson(config);
    }
  }

  async updateAgentReActSteps(agentId: string, reactSteps: ReActStep[]): Promise<void> {
    const metadata = this.agents.get(agentId);
    if (metadata) {
      metadata.reactSteps = cloneJson(reactSteps);
    }
  }

  async deleteAgent(agentId: string): Promise<void> {
    this.agents.delete(agentId);
  }

  clear(): void {
    this.agents.clear();
  }
}
