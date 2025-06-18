# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

sbt clean dist
unzip workflow-compiling-service/target/universal/workflow-compiling-service-0.1.0.zip -d target/
rm workflow-compiling-service/target/universal/workflow-compiling-service-0.1.0.zip

unzip file-service/target/universal/file-service-0.1.0.zip -d target/
rm file-service/target/universal/file-service-0.1.0.zip

unzip config-service/target/universal/config-service-0.1.0.zip -d target/
rm config-service/target/universal/config-service-0.1.0.zip

unzip computing-unit-managing-service/target/universal/computing-unit-managing-service-0.1.0.zip -d target/
rm computing-unit-managing-service/target/universal/computing-unit-managing-service-0.1.0.zip

unzip amber/target/universal/texera-0.1-SNAPSHOT.zip -d amber/target/
rm amber/target/universal/texera-0.1-SNAPSHOT.zip
