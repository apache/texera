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

red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

echo "${red}Terminating Shared Editing Server at $(pgrep -f y-websocket)...${reset}"
kill -9 $(pgrep -f y-websocket-server)
echo "${green}Terminated.${reset}"

echo "${red}Terminating WorkflowCompilingService at $(pgrep -f WorkflowCompilingService)...${reset}"
kill -9 $(pgrep -f WorkflowCompilingService)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating FileService at $(pgrep -f FileService)...${reset}"
kill -9 $(pgrep -f FileService)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating ConfigService at $(pgrep -f ConfigService)...${reset}"
kill -9 $(pgrep -f ConfigService)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating ComputingUnitManagingService at $(pgrep -f ComputingUnitManagingService)...${reset}"
kill -9 $(pgrep -f ComputingUnitManagingService)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating TexeraWebApplication at $(pgrep -f TexeraWebApplication)...${reset}"
kill -9 $(pgrep -f TexeraWebApplication)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating ComputingUnitMaster at $(pgrep -f ComputingUnitMaster)...${reset}"
kill -9 $(pgrep -f ComputingUnitMaster)
echo "${green}Terminated.${reset}"
