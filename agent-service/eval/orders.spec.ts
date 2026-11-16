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

import { describe, expect, test } from "bun:test";
import {
  CATEGORIES,
  countAbove,
  firstAbove,
  generateOrders,
  ordersCsv,
  topCategoriesBySales,
  type Order,
} from "./orders";

describe("generateOrders", () => {
  test("makes the same rows on every call", () => {
    expect(generateOrders()).toEqual(generateOrders());
  });

  test("makes 240 orders by default, every category alike, with positive amounts", () => {
    const orders = generateOrders();
    expect(orders.length).toBe(240);
    for (const category of CATEGORIES) expect(orders.filter(o => o.category === category).length).toBe(40);
    expect(orders.every(o => o.amount > 0)).toBe(true);
    expect(new Set(orders.map(o => o.order_id)).size).toBe(240);
  });

  test("makes no rows when asked for none", () => {
    expect(generateOrders(0)).toEqual([]);
  });
});

describe("the expected answers", () => {
  const orders = generateOrders();

  test("rank the categories by their weights, far enough apart that rounding cannot swap them", () => {
    expect(topCategoriesBySales(orders, 3)).toEqual(["Technology", "Furniture", "Office Supplies"]);
  });

  test("count only the orders strictly above the threshold", () => {
    const few: Order[] = [
      { order_id: 1, order_date: "2024-01-01", category: "Toys", region: "East", amount: 100 },
      { order_id: 2, order_date: "2024-01-01", category: "Toys", region: "East", amount: 100.01 },
    ];
    expect(countAbove(few, 100)).toBe(1);
    const above = countAbove(orders, 100);
    expect(above).toBeGreaterThan(0);
    expect(above).toBeLessThan(orders.length);
  });

  test("find the first order above a threshold, in file order", () => {
    // The order the cases' failing Python operator stops at: its error names this id.
    expect(firstAbove(orders, 250)?.order_id).toBe(100115);
    expect(orders.filter(o => o.order_id < 100115).every(o => o.amount <= 250)).toBe(true);
    expect(firstAbove(orders, 10_000)).toBeUndefined();
  });

  test("handle no orders", () => {
    expect(topCategoriesBySales([], 3)).toEqual([]);
    expect(countAbove([], 100)).toBe(0);
    expect(firstAbove([], 100)).toBeUndefined();
  });
});

describe("ordersCsv", () => {
  test("writes a header, one line per order with two decimals, and a final newline", () => {
    const csv = ordersCsv([
      { order_id: 7, order_date: "2024-02-03", category: "Office Supplies", region: "West", amount: 12.5 },
    ]);
    expect(csv).toBe("order_id,order_date,category,region,amount\n7,2024-02-03,Office Supplies,West,12.50\n");
  });
});
