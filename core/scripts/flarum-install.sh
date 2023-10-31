#!/bin/bash

# Update Homebrew and upgrade packages
read -s -p "Enter your sudo password: " User_PW
echo

echo "Updating Homebrew..."
brew update && brew upgrade

# Install PHP, Apache, mysql-client and Composer
brew install php httpd composer mysql-client


echo "Creating flarum directory..."
echo $User_PW | sudo -S mkdir -p /opt/homebrew/var/www/flarum
sudo chmod -R 775 /opt/homebrew/var/www/flarum
rm -rf /opt/homebrew/var/www/flarum/*
cd /opt/homebrew/var/www/flarum
composer create-project flarum/flarum . --stability=beta

# Copy config files
echo "Copying config files..."
echo $User_PW | sudo -S cp config.php /opt/homebrew/var/www/flarum/config.php
sudo cp .htaccess /opt/homebrew/var/www/flarum/public/.htaccess

# Database Configuration
echo "Setting up mysql database for flarum..."
mysql -u root -p$SQL_PW -h localhost -P 3306 -e "CREATE DATABASE IF NOT EXISTS flarum;"
echo "Dumping necessary SQL files..."
mysql -u root -p$SQL_PW flarum < sql/flarum.sql


# Apache Configuration
HTTPD_CONF="/opt/homebrew/etc/httpd/httpd.conf"
VHOST_CONF="/opt/homebrew/etc/httpd/extra/httpd-vhosts.conf"
PHP_CONF="/opt/homebrew/etc/httpd/extra/httpd-php.conf"

echo "Configuring Apache..."
sudo sed -i '' 's|#LoadModule rewrite_module|LoadModule rewrite_module|' $HTTPD_CONF
sudo sed -i '' 's|#Include /opt/homebrew/etc/httpd/extra/httpd-vhosts.conf|Include /opt/homebrew/etc/httpd/extra/httpd-vhosts.conf|' $HTTPD_CONF
sudo sed -i '' 's|Listen 8080|Listen 80|' $HTTPD_CONF

# Add PHP configuration
echo "LoadModule php_module /opt/homebrew/opt/php/lib/httpd/modules/libphp.so" | sudo tee -a $HTTPD_CONF
echo "Include /opt/homebrew/etc/httpd/extra/httpd-php.conf" | sudo tee -a $HTTPD_CONF


# Check if httpd-php.conf exists, if not, create and configure it
if [ ! -f $PHP_CONF ]; then
    echo "Creating and configuring httpd-php.conf..."
    echo "
<IfModule php_module>
    <FilesMatch \.php$>
        SetHandler application/x-httpd-php
    </FilesMatch>

    <IfModule dir_module>
        DirectoryIndex index.html index.php
    </IfModule>
</IfModule>" | sudo tee $PHP_CONF
fi

# Virtual Host Configuration
echo "
<VirtualHost *:80>
    DocumentRoot \"/opt/homebrew/var/www/flarum/public\"
    <Directory \"/opt/homebrew/var/www/flarum/public\">
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>
</VirtualHost>" | sudo tee -a $VHOST_CONF

# Restart Apache
echo "Restarting Apache..."
sudo apachectl restart
echo "Configuring permissions..."
sudo chown -R _www:_www /opt/homebrew/var/www/flarum

# Publish assets
cd /opt/homebrew/var/www/flarum
echo "Configuring flarum..."
sudo php flarum assets:publish
echo "Flarum installation completed\nYou can now access your flarum forum in Texera"







## ubuntu based script 
# #!/bin/bash

# # Update and upgrade packages
# sudo apt-get update && sudo apt-get upgrade -y

# # Install Apache2
# sudo apt-get install apache2 -y

# # Install PHP and required extensions
# sudo apt install php php-mysql php-gd php-xml php-mbstring php-json php-zip php-curl php-xml composer -y

# # Install MySQL client
# sudo apt-get install mysql-client -y

# cd /var/www/

# composer create-project flarum/flarum . --stability=beta

# #Database Configuration
# # DB_USERNAME="root"
# # DB_PASSWORD="W14zalzhygr$" #Change this to your own password

# #Create Database and User
# # mysql -u root -p -e "CREATE DATABASE ${DB_DATABASE};"
# # mysql -u root -p -e "CREATE USER '${DB_USERNAME}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';"
# # mysql -u root -p -e "GRANT ALL ON ${DB_DATABASE}.* TO '${DB_USERNAME}'@'localhost';"
# # mysql -u root -p -e "FLUSH PRIVILEGES;"

# # Copy .htaccess file
# cp /Users/henryliu/Desktop/flarum-data/htaccess /var/www/flarum/.htaccess

# # Import SQL dump
# mysql -u root -pW14zalzhygr$ -h localhost:3306 flarum < /Users/henryliu/Desktop/flarum-data/flarum.sql

# # Copy configuration file
# cp /Users/henryliu/Desktop/flarum-data/config.php /var/www/flarum/config.php

# # Set permissions
# sudo chown -R www-data:www-data /var/www/flarum
# sudo chmod -R 755 /var/www/flarum

# # Restart Apache
# sudo systemctl restart apache2