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

/**
 * The data the cases work on: a table of orders made by a seeded generator, so every run uploads
 * the same file and every expected answer is computed from the same rows rather than written down.
 */

export const CATEGORIES = ["Technology", "Furniture", "Office Supplies", "Apparel", "Grocery", "Toys"] as const;
const REGIONS = ["East", "West", "South", "Central"] as const;
/** Each category's amounts are scaled by its weight, which keeps the category totals far apart. */
const WEIGHTS = [9, 7, 5, 4, 3, 2];

export interface Order {
  order_id: number;
  order_date: string;
  category: string;
  region: string;
  amount: number;
}

/** A linear congruential generator: the same seed gives the same numbers on every machine. */
function seeded(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

export function generateOrders(count = 240): Order[] {
  const next = seeded(9454);
  const start = Date.UTC(2024, 0, 1);
  return Array.from({ length: count }, (_, i) => {
    const c = i % CATEGORIES.length;
    return {
      order_id: 100001 + i,
      order_date: new Date(start + (i % 365) * 86_400_000).toISOString().slice(0, 10),
      category: CATEGORIES[c],
      region: REGIONS[Math.floor(next() * REGIONS.length)],
      amount: Math.round((5 + next() * 25) * WEIGHTS[c] * 100) / 100,
    };
  });
}

export function ordersCsv(orders: Order[]): string {
  const rows = orders.map(o => [o.order_id, o.order_date, o.category, o.region, o.amount.toFixed(2)].join(","));
  return ["order_id,order_date,category,region,amount", ...rows].join("\n") + "\n";
}

/** The categories with the highest total amount, highest first. */
export function topCategoriesBySales(orders: Order[], n: number): string[] {
  const totals = new Map<string, number>();
  for (const o of orders) totals.set(o.category, (totals.get(o.category) ?? 0) + o.amount);
  return [...totals.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([category]) => category);
}

/** The first order, in file order, whose amount is strictly above the threshold. */
export function firstAbove(orders: Order[], threshold: number): Order | undefined {
  return orders.find(o => o.amount > threshold);
}

/** How many orders have an amount strictly above the threshold. */
export function countAbove(orders: Order[], threshold: number): number {
  return orders.filter(o => o.amount > threshold).length;
}
