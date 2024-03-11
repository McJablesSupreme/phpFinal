<?php 
/*
File: dataConnect.php
 

Description:
Sends a request to the database and returns a JSON string to the client. 
 
*/
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);


/*
 * Will need to implement authentication practices for this script.
 */

define ('DB_USER', 'root');
define ('DB_PASSWORD', '');
define ('DB_HOST', 'localhost');
define ('DB_NAME', 'FinalProject');

$dbc = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME) or die ('Could not connect to MySQL: ' . mysqli_connect_error());
  
