/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useRef, useState } from "react";
import type { SocialAuth } from "./social-auth";

export function GoogleButton({ social }: { social: SocialAuth }) {
  const element = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string>();
  useEffect(() => {
    let active = true;
    let unmount: (() => void) | undefined;
    void social
      .mountGoogle(element.current!, reason => {
        if (active) setError(reason.message);
      })
      .then(dispose => {
        if (active) unmount = dispose;
        else dispose();
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Google sign-in is unavailable.");
      });
    return () => {
      active = false;
      unmount?.();
    };
  }, [social]);
  return (
    <>
      <div ref={element} />
      {error && <p role="alert">{error}</p>}
    </>
  );
}
