#!/bin/bash

echo "Updating Apt..."
sudo apt update && sudo apt upgrade
# Install PHP, Apache, mysql-client and Composer
sudo apt remove apache2
sudo apt install php php php-cli apache2 composer

echo "Creating flarum directory..."
sudo rm -rf /opt/flarum
sudo mkdir /opt/flarum
sudo chown $USER:$USER /opt/flarum
composer create-project flarum/flarum /opt/flarum
composer require --working-dir=/opt/flarum michaelbelgium/flarum-discussion-views
composer require --working-dir=/opt/flarum fof/byobu:"*"
sudo cp ./scripts/config.php /opt/flarum/config.php
sudo cp ./scripts/.htaccess /opt/flarum/public/.htaccess
sudo chown -R www-data:www-data /opt/flarum

# Database Configuration
echo "Setting up mysql database for flarum..."
sudo mysql -u root -p < ./scripts/sql/flarum.sql


# Apache Configuration
HTTPD_CONF="/etc/apache2/apache2.conf"
VHOST_CONF="/etc/apache2/sites-available/flarum.conf"
VHOST_ENABLED="/etc/apache2/sites-enabled/flarum.conf"
PORTS_CONF="/etc/apache2/ports.conf"
# creating config files
sudo touch VHOST_CONF
sudo touch VHOST_ENABLED
# establish symblink under sites-enabled
sudo ln -s VHOST_CONF VHOST_ENABLED
echo "Configuring Apache..."
sudo sed -i '' 's|#IncludeOptional mods-enabled/*.load|IncludeOptional mods-enabled/*.load|' $HTTPD_CONF
sudo sed -i '' 's|#IncludeOptional mods-enabled/*.conf|IncludeOptional mods-enabled/*.conf|' $HTTPD_CONF
sudo sed -i '' 's|Listen 80|Listen 8888|' $PORTS_CONF

# Add PHP configuration
#echo "LoadModule php_module /opt/homebrew/opt/php/lib/httpd/modules/libphp.so" | tee -a $HTTPD_CONF
#echo "Include /opt/homebrew/etc/httpd/extra/httpd-php.conf" | tee -a $HTTPD_CONF


# Virtual Host Configuration
sudo echo "
<VirtualHost *:8888>
    #DocumentRoot \"/opt/flarum/public\"
    #<Directory \"/opt/flarum/public\">
        #Options Indexes FollowSymLinks
        #AllowOverride All
        #Require all granted
    #</Directory>
#</VirtualHost>" | tee -a $VHOST_CONF

# Restart Apache

echo "Restarting Apache..."
sudo systemctl restart apache2

# Publish assets
cd /opt/flarum
echo "Configuring flarum..."

read -p "Enter your database username: " dbusername

# Ask for database password
read -sp "Enter your database password: " dbpassword
echo

# Replace placeholders in the config.php file
sudo chown $USER:$USER /opt/flarum
sed -i "s/'username' => 'REPLACE_WITH_YOUR_USERNAME'/'username' => '$dbusername'/g" /opt/flarum/config.php
sed -i "s/'password' => 'REPLACE_WITH_YOUR_PASSWORD'/'password' => '$dbpassword'/g" /opt/flarum/config.php

echo "Configuration updated."
sudo chown -R www-data:www-data /opt/flarum
sudo php flarum assets:publish
echo "Flarum installation completed\nYou can now access your flarum forum in Texera"