<?php return array (
  'debug' => false,
  'database' =>
  array (
    'driver' => 'mysql',
    'host' => 'localhost',
    'port' => 3306,
    'database' => 'flarum',
    'username' => 'REPLACE_WITH_YOUR_USERNAME',
    'password' => 'REPLACE_WITH_YOUR_PASSWORD',
    'charset' => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'prefix' => '',
    'strict' => false,
    'engine' => 'InnoDB',
    'prefix_indexes' => true,
  ),
  'url' => 'http://localhost:8888',
  'paths' =>
  array (
    'api' => 'api',
    'admin' => 'admin',
  ),
  'headers' =>
  array (
    'poweredByHeader' => true,
    'referrerPolicy' => 'same-origin',
  ),
);
