
$currentDir = (pwd).Path + '\Apache24'; (Get-Content ./Apache24/conf/httpd.conf) -replace 'c:/Apache24', $currentDir | Set-Content ./Apache24/conf/httpd.conf