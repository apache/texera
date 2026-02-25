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

package org.apache.texera.amber.pybuilder

import io.github.classgraph.ClassGraph

import java.lang.reflect.Modifier
import scala.jdk.CollectionConverters._

private[amber] object PythonClassgraphScanner {

  def scanCandidates(
      base: Class[_],
      acceptPackages: Seq[String],
      classLoader: ClassLoader
  ): Seq[Class[_]] = {
    val cg = new ClassGraph()
      .overrideClassLoaders(classLoader)
      .enableClassInfo()

    acceptPackages.foreach(p => cg.acceptPackages(p))

    val scanResult = cg.scan()
    try {
      val infoList =
        if (base.isInterface) scanResult.getClassesImplementing(base.getName)
        else scanResult.getSubclasses(base.getName)

      infoList
        .loadClasses()
        .asScala
        .toSeq
        .filterNot(_.isInterface)
        .filterNot(c => Modifier.isAbstract(c.getModifiers))
    } finally {
      scanResult.close()
    }
  }
}
