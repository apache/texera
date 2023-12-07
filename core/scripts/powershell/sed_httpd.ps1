
Write-Host "configuring apache2"
$HTTPD_CONF = "C:\Users\Administrator\AppData\Roaming\Apache24\conf\httpd.conf"
$VHOST_CONF = "C:\Users\Administrator\AppData\Roaming\Apache24\conf\extra\httpd-vhosts.conf"
$PHP_CONF = "C:\Users\Administrator\AppData\Roaming\Apache24\conf\extra\httpd-php.conf"

# Replace content in httpd.conf
(Get-Content $HTTPD_CONF) -replace '#LoadModule rewrite_module', 'LoadModule rewrite_module' | Set-Content $HTTPD_CONF
(Get-Content $HTTPD_CONF) -replace '#LoadModule headers_module', 'LoadModule headers_module' | Set-Content $HTTPD_CONF
(Get-Content $HTTPD_CONF) -replace '#LoadModule deflate_module', 'LoadModule deflate_module' | Set-Content $HTTPD_CONF
(Get-Content $HTTPD_CONF) -replace '# Include conf/extra/httpd-vhosts.conf', 'Include conf/extra/httpd-vhosts.conf' | Set-Content $HTTPD_CONF
(Get-Content $HTTPD_CONF) -replace 'Listen 8080', 'Listen 8888' | Set-Content $HTTPD_CONF
$PhpModuleLine = 'LoadModule php_module "C:\PHP\php8apache2_4.dll"'

Add-Content -Path $HTTPD_CONF -Value $PhpModuleLine
$PhpModuleLine1 = 'Include conf/extra/httpd-php.conf'

Add-Content -Path $HTTPD_CONF -Value $PhpModuleLine1
$PhpModuleLine2 = 'PHPIniDir "C:/php"'

Add-Content -Path $HTTPD_CONF -Value $PhpModuleLine2


# Check if httpd-php.conf exists, if not, create and configure it
if (-Not (Test-Path $PHP_CONF)) {
    Write-Host "Creating and configuring httpd-php.conf..."
    @"
<IfModule php_module>
    <FilesMatch \.php$>
        SetHandler application/x-httpd-php
    </FilesMatch>

    <IfModule dir_module>
        DirectoryIndex index.html index.php
    </IfModule>
</IfModule>
"@ | Set-Content $PHP_CONF
}

# Virtual Host Configuration
@"
<VirtualHost *:8888>
    DocumentRoot "c:\flarum\public"
    <Directory "c:\flarum\public">
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>
</VirtualHost>
"@ | Add-Content $VHOST_CONF

# Restart Apache
Write-Host "Restarting Apache..."
Restart-Service -Name "Apache"

# Publish assets
Set-Location c:\flarum
Write-Host "Configuring flarum..."
$dbUsername = Read-Host -Prompt "Enter your database username"
$dbPassword = Read-Host -Prompt "Enter your database password"
$configPath = "config.php"
$configContent = Get-Content $configPath -Raw

# Replace placeholders with actual values
$configContent = $configContent -replace "'username' => 'REPLACE_WITH_YOUR_USERNAME'", "'username' => '$dbUsername'"
$configContent = $configContent -replace "'password' => 'REPLACE_WITH_YOUR_PASSWORD'", "'password' => '$dbPassword'"

# Write the updated content back to the config file
$configContent | Set-Content $configPath
php flarum assets:publish
Write-Host "Flarum installation completed. You can now access your flarum forum."
Write-Host "Please go to http://localhost:8888/"