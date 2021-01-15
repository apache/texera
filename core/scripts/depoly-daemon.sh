red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

echo "${green}Compiling Amber...${reset}"
cd amber && sbt clean compile
echo "${green}Amber compiled.${reset}"
echo

echo "${green}Compiling GUI...${reset}"
cd ../new-gui && ng build --prod  --deploy-url=/ --base-href=/ 
echo "${green}GUI compiled.${reset}"
echo

cd ..
echo "${green}Starting TexeraWebApplication in daemon...${reset}"
setsid nohup ./scripts/server.sh >> log/server.log 2>&1 &
echo "${green}Waiting TexeraWebApplication to launch on 8080...${reset}"
while ! nc -z localhost 8080; do   
	sleep 0.1 # wait for 1/10 of the second before check again
done
echo "${green}TexeraWebApplication launched at $(pgrep -f TexeraWebApplication)${reset}"
echo

echo "${green}Starting TexeraRunWorker in daemon...${reset}"
setsid nohup ./scripts/worker.sh >> log/worker.log 2>&1 &
echo "${green}TexeraRunWorker launched at $(pgrep -f TexeraRunWorker)${reset}"
