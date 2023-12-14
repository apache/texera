#!/bin/bash

echo "Updating Apt..."
sudo apt update && sudo apt upgrade
# Install PHP, Apache, mysql-client and Composer
sudo apt install php apache2 composer

echo "Creating flarum directory..."
rm -rf /opt/flarum
mkdir /opt/flarum
composer create-project flarum/flarum /opt/flarum
composer require --working-dir=/opt/flarum michaelbelgium/flarum-discussion-views
composer require --working-dir=/opt/flarum fof/byobu:"*"
cp config.php /opt/flarum/config.php
cp .htaccess /opt/flarum/public/.htaccess

# Database Configuration
echo "Setting up mysql database for flarum..."
mysql -u root -p < sql/flarum.sql


# Apache Configuration
HTTPD_CONF="/opt/homebrew/etc/httpd/httpd.conf"
VHOST_CONF="/opt/homebrew/etc/httpd/extra/httpd-vhosts.conf"
PHP_CONF="/opt/homebrew/etc/httpd/extra/httpd-php.conf"

echo "Configuring Apache..."
sed -i '' 's|#LoadModule rewrite_module|LoadModule rewrite_module|' $HTTPD_CONF
sed -i '' 's|#Include /opt/homebrew/etc/httpd/extra/httpd-vhosts.conf|Include /opt/homebrew/etc/httpd/extra/httpd-vhosts.conf|' $HTTPD_CONF
sed -i '' 's|Listen 8080|Listen 8888|' $HTTPD_CONF

# Add PHP configuration
echo "LoadModule php_module /opt/homebrew/opt/php/lib/httpd/modules/libphp.so" | tee -a $HTTPD_CONF
echo "Include /opt/homebrew/etc/httpd/extra/httpd-php.conf" | tee -a $HTTPD_CONF


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
</IfModule>" | tee $PHP_CONF
fi

# Virtual Host Configuration
echo "
<VirtualHost *:8888>
    DocumentRoot \"/opt/flarum/public\"
    <Directory \"/opt/flarum/public\">
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>
</VirtualHost>" | tee -a $VHOST_CONF

# Restart Apache
echo "Restarting Apache..."
sudo apachectl restart

# Publish assets
cd /opt/flarum
echo "Configuring flarum..."
php flarum assets:publish
sudo chown -R _www:_www /opt/flarum
echo "Flarum installation completed\nYou can now access your flarum forum in Texera"