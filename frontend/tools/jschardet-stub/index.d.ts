/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

export interface IDetectedMap {
  encoding: string;
  confidence: number;
}

export interface IOptionsMap {
  minimumThreshold?: number;
  detectEncodings?: Array<string>;
}

export declare function detect(buffer: Buffer | string, options?: IOptionsMap): IDetectedMap | null;

export declare function detectAll(buffer: Buffer | string, options?: IOptionsMap): IDetectedMap[];

export declare function enableDebug(): void;
