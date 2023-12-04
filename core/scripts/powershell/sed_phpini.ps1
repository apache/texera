(Get-Content 'C:\PHP\php.ini') -replace ';extension=fileinfo', 'extension=fileinfo' | Set-Content 'C:\PHP\php.ini'
(Get-Content 'C:\PHP\php.ini') -replace ';extension=pdo_mysql', 'extension=pdo_mysql' | Set-Content 'C:\PHP\php.ini'

