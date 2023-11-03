red=$(tput setaf 1)
green=$(tput setaf 2)
reset=$(tput sgr0)

skipCompilation=false
while getopts s flag
do
    case "${flag}" in
        s) skipCompilation=true;;
        *) exit 1
    esac
done

if  ! $skipCompilation
then
  echo "${green}Compiling Amber...${reset}"
  cd amber && sbt clean package
  echo "${green}Amber compiled.${reset}"
  echo

  echo "${green}Compiling GUI...${reset}"
  cd ../new-gui && yarn install && ng build --prod  --deploy-url=/ --base-href=/
  echo "${green}GUI compiled.${reset}"
  echo
  cd ..
fi

echo "${green}Starting TexeraWebApplication in daemon...${reset}"
setsid nohup ./scripts/server.sh >/dev/null 2>&1 &
echo "${green}Waiting TexeraWebApplication to launch on 8080...${reset}"
while ! nc -z localhost 8080; do   
	sleep 0.1 # wait 100ms before check again
done
echo "${green}TexeraWebApplication launched at $(pgrep -f TexeraWebApplication)${reset}"
echo

echo "${green}Starting TexeraRunWorker in daemon...${reset}"
setsid nohup ./scripts/worker.sh >/dev/null 2>&1 &
sleep 0.2 # wait for 200ms to get the pid
echo "${green}TexeraRunWorker launched at $(pgrep -f TexeraRunWorker)${reset}"

echo "${green}Starting shared editing server...${reset}"
setsid nohup ./scripts/shared-editing-server.sh >/dev/null 2>&1 &
sleep 2
echo "${green}Shared Editing Server launched at $(pgrep -f y-websocket)${reset}"
