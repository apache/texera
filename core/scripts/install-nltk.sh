unameOut="$(uname -s)"
case "${unameOut}" in
	Linux*) 
		machine=Linux
		OS=$(lsb_release -si)
		VER=$(lsb_release -sr)
		if [ $OS = "Ubuntu" ]; then
			machine="UBUNTU"
			sudo apt-get install python3
			pip3 install nltk
		fi
		;;
	Darwin*)
		machine=Mac
		brew install python3
		pip3 install nltk
		;;
esac
