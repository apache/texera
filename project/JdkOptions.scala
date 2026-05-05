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
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * --add-opens flags required to run forked test JVMs under JDK 17+.
 * Mirrors Apache Pekko's project/JdkOptions.scala
 * (https://github.com/apache/pekko/blob/main/project/JdkOptions.scala),
 * with one addition for Ehcache `SizeOf` (used by `Tuple.inMemSize`)
 * tracked by discussion #4001.
 */
object JdkOptions {

  lazy val versionSpecificJavaOptions: List[String] =
    // Apache Pekko: Agrona UnsafeApi, virtual threads, dispatcher/mailbox primitives
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED" ::
    // Apache Pekko: virtual threads. Also Ehcache SizeOf reflection (Tuple.inMemSize).
    "--add-opens=java.base/java.lang=ALL-UNNAMED" ::
    // Apache Pekko: Aeron / NIO channel access
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" ::
    // Apache Pekko: LevelDB. Also Apache Arrow Java off-heap memory.
    "--add-opens=java.base/java.nio=ALL-UNNAMED" ::
    // Ehcache SizeOf reflection (Tuple.inMemSize)
    "--add-opens=java.base/java.util=ALL-UNNAMED" :: Nil
}
