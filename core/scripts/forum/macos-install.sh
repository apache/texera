#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


echo "Updating Homebrew..."
brew update && brew upgrade
# Install PHP, Apache, mysql-client and Composer
brew install php httpd composer

echo "Creating flarum directory..."
rm -rf /opt/homebrew/var/www/flarum
mkdir /opt/homebrew/var/www/flarum
composer create-project flarum/flarum /opt/homebrew/var/www/flarum
composer require --working-dir=/opt/homebrew/var/www/flarum michaelbelgium/flarum-discussion-views
composer require --working-dir=/opt/homebrew/var/www/flarum fof/byobu:"*"
cp config.php /opt/homebrew/var/www/flarum/config.php
cp .htaccess /opt/homebrew/var/www/flarum/public/.htaccess

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
    DocumentRoot \"/opt/homebrew/var/www/flarum/public\"
    <Directory \"/opt/homebrew/var/www/flarum/public\">
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>
</VirtualHost>" | tee -a $VHOST_CONF

# Restart Apache
echo "Restarting Apache..."
sudo apachectl restart

# Publish assets
cd /opt/homebrew/var/www/flarum
echo "Configuring flarum..."
php flarum assets:publish
sudo chown -R _www:_www /opt/homebrew/var/www/flarum
echo "Flarum installation completed\nYou can now access your flarum forum in Texera"