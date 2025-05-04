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
 * make sure do not add const/declare before enum here.
 * Const enums are removed during transpiration in JS so you can not use them at runtime.
 * Source: https://stackoverflow.com/questions/50365598/typescript-runtime-error-cannot-read-property-of-undefined-enum
 */
export enum GenericWebResponseCode {
  SUCCESS = 0,
}

export interface GenericWebResponse
  extends Readonly<{
    code: GenericWebResponseCode;
    message: string;
  }> {}
