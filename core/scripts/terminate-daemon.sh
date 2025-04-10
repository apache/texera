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

echo "${red}Terminating TexeraWebApplication at $(pgrep -f TexeraWebApplication)...${reset}"
kill -9 $(pgrep -f TexeraWebApplication)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating ComputingUnitMaster at $(pgrep -f ComputingUnitMaster)...${reset}"
kill -9 $(pgrep -f ComputingUnitMaster)
echo "${green}Terminated.${reset}"
