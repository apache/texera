Write-Host "Setting up mysql database for flarum..."
mysql -u root -p -e "source ./scripts/sql/flarum.sql"
