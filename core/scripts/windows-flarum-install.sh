echo "please run this script as an administrator(in elevated prompt, which means you
should right click on gitbash and click run this as an administrator)"
#installing choco

#read -p "have you installed choco?(Y/N):" answer
#if [[ $answer == "N" || $answer == "n" ]]
#then
    #powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/installchoco.ps1
#fi

#installing apache2, php,httpd, composer
echo "installing apache2"
powershell.exe -Command "& {choco install apache-httpd}"
echo "installing php"
powershell.exe -Command "& {choco install php --package-parameters='"/ThreadSafe ""/InstallDir:C:\\PHP"""'}"
#sed -i '' 's|;extension=fileinfo|extension=fileinfo|' "c:\\PHP\\php.ini"
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/sed_phpini.ps1
#read -p "have you installed composer?(Y/N):" answer1
#if [[ $answer1 == "N" || $answer1 == "n" ]]
#then
    #curl -o Composer-Setup.exe https://getcomposer.org/Composer-Setup.exe
    #powershell.exe -Command "& {Start-Process .\Composer-Setup.exe -Wait}"
    #rm Composer-Setup.exe
#fi

#setting up database
#echo "Setting up mysql database for flarum..."
#mysql -u root -p 
#source ./scripts/sql/flarum.sql
#exit 


#curl -O https://www.apachelounge.com/download/VS17/binaries/httpd-2.4.58-win64-VS17.zip

#create flarum
echo "Creating flarum directory..."
rm -rf c:\\flarum
mkdir c:\\flarum
echo "Creating flarum project"
powershell.exe -Command "& {composer create-project flarum/flarum c:\\flarum}"
echo "downloading view extension"
current_dir=$(pwd)
echo $current_dir
cd c:\\flarum
echo "we are at" $(pwd)
powershell.exe -Command "& {composer require michaelbelgium/flarum-discussion-views}"
echo "downloading byobu extension"
powershell.exe -Command "& {composer require fof/byobu:'*'}"
cd $current_dir
cp ./scripts/config.php c:\\flarum\\config.php
cp ./scripts/.htaccess c:\\flarum\\public\\.htaccess

#apache2 configuration
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/sed_httpd.ps1