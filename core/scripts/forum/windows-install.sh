echo "please run this script as an administrator(in elevated prompt, which means you
should right click on gitbash and click run this as an administrator)"
#installing apache2, php,httpd, composer

echo "installing apache2"
curl -O "https://www.apachelounge.com/download/VS17/binaries/httpd-2.4.58-win64-VS17.zip"
unzip httpd-2.4.58-win64-VS17.zip > /dev/null 2>&1
rm -- "-- Win64 VS17  --"
rm httpd-2.4.58-win64-VS17.zip
rm ReadMe.txt


echo "replace apache2 serverroot"
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/replaceroot.ps1 


echo "installing php"
curl -O "https://windows.php.net/downloads/releases/php-8.3.1-Win32-vs16-x64.zip"
mkdir php
cd php
unzip ../php-8.3.1-Win32-vs16-x64.zip > /dev/null 2>&1
cd ..
rm php-8.3.1-Win32-vs16-x64.zip


echo "installing composer"
curl -o Composer-Setup.exe https://getcomposer.org/Composer-Setup.exe
powershell.exe -Command "& {Start-Process .\Composer-Setup.exe -Wait}"
rm Composer-Setup.exe


echo "reading your database username and password"

# Regular expressions
username_re="^ *username *= *\"([^\"]+)\""
password_re="^ *password *= *\"([^\"]+)\""

# Read and match username and password
while read -r line; do
    if [[ $line =~ $username_re ]]; then
        username=${BASH_REMATCH[1]}
        echo "Username: $username"
    elif [[ $line =~ $password_re ]]; then
        password=${BASH_REMATCH[1]}
        echo "Password: $password"
    fi
done < ./amber/src/main/resources/application.conf

#setting up database
echo "Setting up mysql database for flarum..."
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/configuremysql.ps1
rm Composer-Setup.exe

#create flarum
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/sed_phpini.ps1
echo "Creating flarum directory..."
rm -rf c:\\flarum
mkdir c:\\flarum
echo "Creating flarum project"
powershell.exe -Command "& {composer create-project flarum/flarum c:\\flarum}"
echo "downloading view extension"
current_dir=$(pwd)
echo $current_dir
cd c:\\flarum
powershell.exe -Command "& {composer require michaelbelgium/flarum-discussion-views}"
echo "downloading byobu extension"
powershell.exe -Command "& {composer require fof/byobu:'*'}"
cd $current_dir
cp ./scripts/config.php c:\\flarum\\config.php
cp ./scripts/.htaccess c:\\flarum\\public\\.htaccess

#apache2 configuration
powershell.exe -ExecutionPolicy Bypass -File ./scripts/powershell/sed_httpd.ps1 $username $password

#start apache2
./Apache24/bin/httpd.exe