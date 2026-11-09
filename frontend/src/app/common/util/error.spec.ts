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

import { extractErrorMessage } from "./error";

describe("extractErrorMessage", () => {
  it("should return the message from an Error instance", () => {
    const testError = new Error("error instance");
    const result = extractErrorMessage(testError);
    expect(result).toBe(testError.message);
  });

  it("should return the error string when error is a plain string", () => {
    const testError = { error: "boom" };
    const result = extractErrorMessage(testError);
    expect(result).toBe(testError.error);
  });

  it("should return the nested message when error is an object with a message", () => {
    const testError = { error: { message: "nested" } };
    const result = extractErrorMessage(testError);
    expect(result).toBe(testError.error.message);
  });

  it("should return the fallback message for when error is null", () => {
    const testError = null;
    const result = extractErrorMessage(testError);
    expect(result).toBe("An unknown error occurred.");
  });

  it("should return the fallback message when error is a number", () => {
    const testError = 123;
    const result = extractErrorMessage(testError);
    expect(result).toBe("An unknown error occurred.");
  });

  it("should return the fallback message when error is an empty object", () => {
    const testError = {};
    const result = extractErrorMessage(testError);
    expect(result).toBe("An unknown error occurred.");
  });

  it("should return the fallback message when error key value is neither a string nor an object with a message", () => {
    const testError = { error: 123 };
    const result = extractErrorMessage(testError);
    expect(result).toBe("An unknown error occurred.");
  });

  it("should return the fallback message when error key value is null", () => {
    const testError = { error: null };
    const result = extractErrorMessage(testError);
    expect(result).toBe("An unknown error occurred.");
  });
});
