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

const BYTES_PER_UNIT = 1024;
const SIZE_UNITS = ["B", "KB", "MB", "GB", "TB"];

export const formatSize = (bytes?: number): string => {
  if (bytes === undefined || bytes <= 0) return "0 B";

  const unitIndex = Math.min(Math.floor(Math.log(bytes) / Math.log(BYTES_PER_UNIT)), SIZE_UNITS.length - 1);
  const size = bytes / Math.pow(BYTES_PER_UNIT, unitIndex);

  return `${size.toFixed(2)} ${SIZE_UNITS[unitIndex]}`;
};
