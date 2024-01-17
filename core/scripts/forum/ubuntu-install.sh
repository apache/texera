#!/bin/bash

sudo apt update && sudo apt upgrade
sudo apt install apache2 php php-curl php-dom php-mysql

sudo rm -rf /opt/flarum
sudo mkdir /opt/flarum
sudo chown $USER:$USER /opt/flarum

php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"
php composer-setup.php
php -r "unlink('composer-setup.php');"

composer create-project flarum/flarum /opt/flarum
composer require --working-dir=/opt/flarum michaelbelgium/flarum-discussion-views
composer require --working-dir=/opt/flarum fof/byobu:"*"
sudo cp ./scripts/config.php /opt/flarum/config.php
sudo cp ./scripts/.htaccess /opt/flarum/public/.htaccess
sudo chown -R www-data:www-data /opt/flarum
sudo mysql -u root -p < ./scripts/sql/flarum.sql

VHOST_CONF="/etc/apache2/sites-available/flarum.conf"
sudo touch VHOST_CONF
sudo echo "
<VirtualHost *:80>
    DocumentRoot \"/opt/flarum/public\"
    <Directory \"/opt/flarum/public\">
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>
#</VirtualHost>" | sudo tee -a $VHOST_CONF

sudo a2ensite flarum.conf
sudo service apache2 reload

cd /opt/flarum

read -p "Enter your database username: " dbusername

# Ask for database password
read -sp "Enter your database password: " dbpassword
echo

# Replace placeholders in the config.php file
sudo chown $USER:$USER /opt/flarum
sed -i "s/'username' => 'REPLACE_WITH_YOUR_USERNAME'/'username' => '$dbusername'/g" /opt/flarum/config.php
sed -i "s/'password' => 'REPLACE_WITH_YOUR_PASSWORD'/'password' => '$dbpassword'/g" /opt/flarum/config.php
sudo chown -R www-data:www-data /opt/flarum
sudo php flarum assets:publish
echo "Flarum installation completed\nYou can now access your flarum forum in Texera"