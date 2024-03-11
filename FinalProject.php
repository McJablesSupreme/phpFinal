<?php

//include file to connect to the database
include("dataConnectFINAL.php"); 

// display errors
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

//*************** FUNCTIONS ***************//

// Create the table that'll hold merged customer data
function CreateMergedCustomersTable($dbc)
{
    $query = "CREATE TABLE CUSTOMERS (
                CUSTOMER_ID INT AUTO_INCREMENT PRIMARY KEY,
                OLD_KEY VARCHAR(70),
                NAME VARCHAR(70),
                ADDRESS VARCHAR(50),
                CITY VARCHAR(50),
                STATE CHAR(20),
                ZIP INT(5),
                CONTACT_NAME VARCHAR(50),
                CONTACT_PHONE VARCHAR(12),
                ACCT_OPEN_DATE DATE);
            ";
    // catch any errors and display them
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>CUSTOMERS table created successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error creating CUSTOMERS: " . $e->getMessage() . "</p>";
    
    }
}

// Create a table with abbreviation mappings
function CreateStateMappingTable($dbc)
{
    $query = "CREATE TABLE STATE_MAPPING (
                ABBREVIATION CHAR(2) PRIMARY KEY,
                FULL_NAME VARCHAR(255) NOT NULL
            );
                ";

    // catch any errors and display them        
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>STATE_MAPPING table created successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error creating STATE_MAPPING: " . $e->getMessage() . "</p>";
    }
}

// Insert state abbreviation mappings into the new table
// This is necessary because the state names are spelled out in the old tables
// and I want consistency in the new table
// It's also used to for querying purposes since the jewel tables use
// concatenated fields as a key, and I need to match them with the hawkeye tables
function InsertStateMappings($dbc)
{
    $query = "INSERT INTO STATE_MAPPING (ABBREVIATION, FULL_NAME)
                VALUES
                    ('AL', 'Alabama'),
                    ('AK', 'Alaska'),
                    ('AZ', 'Arizona'),
                    ('AR', 'Arkansas'),
                    ('CA', 'California'),
                    ('CO', 'Colorado'),
                    ('CT', 'Connecticut'),
                    ('DE', 'Delaware'),
                    ('FL', 'Florida'),
                    ('GA', 'Georgia'),
                    ('HI', 'Hawaii'),
                    ('ID', 'Idaho'),
                    ('IL', 'Illinois'),
                    ('IN', 'Indiana'),
                    ('IA', 'Iowa'),
                    ('KS', 'Kansas'),
                    ('KY', 'Kentucky'),
                    ('LA', 'Louisiana'),
                    ('ME', 'Maine'),
                    ('MD', 'Maryland'),
                    ('MA', 'Massachusetts'),
                    ('MI', 'Michigan'),
                    ('MN', 'Minnesota'),
                    ('MS', 'Mississippi'),
                    ('MO', 'Missouri'),
                    ('MT', 'Montana'),
                    ('NE', 'Nebraska'),
                    ('NV', 'Nevada'),
                    ('NH', 'New Hampshire'),
                    ('NJ', 'New Jersey'),
                    ('NM', 'New Mexico'),
                    ('NY', 'New York'),
                    ('NC', 'North Carolina'),
                    ('ND', 'North Dakota'),
                    ('OH', 'Ohio'),
                    ('OK', 'Oklahoma'),
                    ('OR', 'Oregon'),
                    ('PA', 'Pennsylvania'),
                    ('RI', 'Rhode Island'),
                    ('SC', 'South Carolina'),
                    ('SD', 'South Dakota'),
                    ('TN', 'Tennessee'),
                    ('TX', 'Texas'),
                    ('UT', 'Utah'),
                    ('VT', 'Vermont'),
                    ('VA', 'Virginia'),
                    ('WA', 'Washington'),
                    ('WV', 'West Virginia'),
                    ('WI', 'Wisconsin'),
                    ('WY', 'Wyoming'),
                    ('DC', 'District of Columbia')
                    ";

    // catch any errors and display them
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>State mappings inserted successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error inserting state mappings: " . $e->getMessage() . "</p>";
    }
}

// Combine the data from the two tables into the new table
function MergeCustomerData($dbc)
{
    $query = "INSERT INTO CUSTOMERS 
    (SELECT 
        hc_customer AS CUSTOMER_ID,
        NULL AS OLD_KEY, 
        hc_name AS NAME, 
        hc_address AS ADDRESS, 
        hc_city AS CITY, 
        sm.abbreviation AS STATE, 
        hc_zip AS ZIP,
        hc_contact AS CONTACT_NAME,
        NULL AS CONTACT_PHONE,
        NULL AS ACCT_OPEN_DATE
    FROM 
        hawkeye_customer hc
    JOIN 
        STATE_MAPPING SM ON hc.hc_state = SM.FULL_NAME
    UNION
    SELECT
        NULL AS CUSTOMER_ID,
        company_code AS OLD_KEY,
        company_name AS NAME,   
        company_address AS ADDRESS,
        company_city AS CITY,
        company_state AS STATE,
        company_zip AS ZIP,
        company_contact AS CONTACT_NAME,
        company_contact_phone AS CONTACT_PHONE,  
        STR_TO_DATE(company_customer_account_opened, '%m/%d/%Y') AS ACCT_OPEN_DATE
    FROM
        jewel_company);
    ";

    // catch any errors and display them
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>Customer data merged successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error merging customer data: " . $e->getMessage() . "</p>";
    }
}

// Create the new TRANSACTIONS table to store the merged transaction data
function CreateMergedTransactionsTable($dbc)
{
    $query = "CREATE TABLE TRANSACTIONS (
                TRANSACTION_ID INT AUTO_INCREMENT PRIMARY KEY,
                OLD_TRANS_ID INT,
                OLD_KEY VARCHAR(70),
                PRODUCT_ID INT,
                OLD_PROD_DESC VARCHAR(255),
                CUSTOMER_ID INT,
                TRANSACTION_DATE VARCHAR(10),
                AMOUNT_PURCHASED INT(3)
            );
        ";

    // catch any errors and display them
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>TRANSACTIONS table created successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error creating TRANSACTIONS table: " . $e->getMessage() . "</p>";
    }
}

// Populate the new TRANSACTIONS table with data from the old tables
function MergeTransactionData($dbc)
{
    $query = "INSERT INTO TRANSACTIONS 
                (SELECT 
                    NULL AS TRANSACTION_ID,
                    h_transaction_id AS OLD_TRANS_ID,
                    NULL AS OLD_KEY,
                    h_product_id AS PRODUCT_ID,
                    NULL AS OLD_PROD_DESC,
                    h_customer_id AS CUSTOMER_ID,
                    h_purchase_date AS TRANSACTION_DATE,
                    h_amt AS AMOUNT_PURCHASED
                FROM 
                    hawkeye_transaction HT
                UNION
                SELECT
                    NULL AS TRANSACTION_ID,
                    jt.transaction_id AS OLD_TRANS_ID,
                    jt.customer_id AS OLD_KEY,
                    NULL AS PRODUCT_ID,
                    jt.product_line AS OLD_PROD_DESC,
                    c.CUSTOMER_ID AS CUSTOMER_ID,
                    STR_TO_DATE(jt.purchase_date, '%m/%d/%Y') AS TRANSACTION_DATE,
                    jt.AMT AS AMOUNT_PURCHASED
                FROM
                    jewel_transactions jt
                JOIN
                    CUSTOMERS c ON jt.customer_id = c.OLD_KEY
                );
    ";

    // catch any errors and display them            
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>Transactions data merged successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error merging transactions data: " . $e->getMessage() . "</p>";
    }
}

// Create a new table to store the merged product data
function CreateMergedProductsTable($dbc)
{
    $query = "CREATE TABLE PRODUCTS (
                PRODUCT_ID INT AUTO_INCREMENT PRIMARY KEY,
                PRODUCT_LINE VARCHAR(25),
                CODE_NAME VARCHAR(15),
                NOTES VARCHAR(2000)
            );
    ";

    // catch any errors and display them        
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>PRODUCTS table created successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error creating PRODUCTS table: " . $e->getMessage() . "</p>";
    }
}

// Populate the new PRODUCTS table with data from the old tables
function MergeProductData($dbc)
{
    $query = "INSERT INTO PRODUCTS 
                (SELECT 
                    product_id AS PRODUCT_ID,
                    product_line AS PRODUCT_LINE,
                    product_code_name AS CODE_NAME,
                    product_notes AS NOTES
                FROM 
                    hawkeye_product
                UNION
                SELECT
                    NULL AS PRODUCT_ID,
                    product_line AS PRODUCT_LINE,
                    NULL AS CODE_NAME,
                    product_notes AS NOTES
                FROM
                    jewel_offerings
            );
        ";

    // catch any errors and display them        
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>Products data merged successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error merging products data: " . $e->getMessage() . "</p>";
    }
}

// Create a new table to store the merged contact data
function CreateMergedContactsTable($dbc)
{
    $query = "CREATE TABLE CONTACTS (
                CONTACT_ID INT AUTO_INCREMENT PRIMARY KEY,
                FIRST_NAME VARCHAR(20),
                LAST_NAME VARCHAR(30),
                CUSTOMER_ID INT REFERENCES CUSTOMERS(CUSTOMER_ID),
                NOTES VARCHAR(744),
                PHONE VARCHAR(12),
                DOB VARCHAR(10)
            );
        ";

    // catch any errors and display them       
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>CONTACTS table created successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error creating CONTACTS table: " . $e->getMessage() . "</p>";
    }
}

// Populate the new CONTACTS table with data from the old tables
function MergeContactData($dbc)
{
    $query = "INSERT INTO CONTACTS 
                (SELECT 
                    hc_contact AS CONTACT_ID,
                    hc_fname AS FIRST_NAME,
                    hc_lname AS LAST_NAME,
                    hc_customer AS CUSTOMER_ID,
                    hc_notes AS NOTES,
                    hc_phone AS PHONE,
                    hc_birthday AS DOB
                FROM 
                    hawkeye_contacts
                UNION
                SELECT
                    NULL AS CONTACT_ID,
                    SUBSTRING(jc.company_contact, 1, -- split into first and last name
                        LOCATE(' ', jc.company_contact)) 
                        AS FIRST_NAME,
                    SUBSTRING(jc.company_contact, 
                        LOCATE(' ', jc.company_contact) + 1, LENGTH(jc.company_contact)) 
                        AS LAST_NAME,
                    c.CUSTOMER_ID AS CUSTOMER_ID,
                    NULL AS NOTES,
                    jc.company_contact_phone AS CONTACT_PHONE,
                    NULL AS DOB
                FROM
                    jewel_company jc
                JOIN
                    CUSTOMERS c ON jc.company_code = c.OLD_KEY);
        ";

    // catch any errors and display them
    try {
        if (mysqli_query($dbc, $query)) {
            echo "<p>Contact data merged successfully</p>";
        } else {
            throw new Exception(mysqli_error($dbc));
        }
    } catch (Exception $e) {
        echo "<p style='color:red';>Error merging contact data: " . $e->getMessage() . "</p>";
    }
}

// Clean up the tables to remove any unnecessary data and columns and finish the merge
function CleanUpTables($dbc)
{
    $queries = [
        // Update the state column in the CUSTOMERS table
        // Needed to create concatenated key for matching in the TRANSACTIONS table
        "UPDATE hawkeye_customer hc JOIN state_mapping sm ON hc.hc_state = sm.FULL_NAME 
            SET hc.hc_state = sm.abbreviation;",

        // only one product was a duplicate
        "DELETE FROM PRODUCTS WHERE PRODUCT_ID = 1012",

        // update the product_id in the TRANSACTIONS table to match the new PRODUCTS table
        "UPDATE TRANSACTIONS SET PRODUCT_ID = 1001 WHERE OLD_PROD_DESC = 'Zaam Whapper'",
        "UPDATE TRANSACTIONS SET PRODUCT_ID = 1010 WHERE OLD_PROD_DESC = 'Impact Spartan'",
        "UPDATE TRANSACTIONS SET PRODUCT_ID = 1011 WHERE OLD_PROD_DESC = 'Timberwolf 2.0'",

        // Update the CUSTOMER_ID in the TRANSACTIONS table if 
        // the customer existed in both old tables
        "UPDATE TRANSACTIONS T
            SET T.CUSTOMER_ID = (
                SELECT hc.hc_customer 
                FROM hawkeye_customer hc 
                WHERE CONCAT(hc.hc_name, hc.hc_address, hc.hc_city, hc.hc_state) = t.OLD_KEY
            )
            WHERE EXISTS (
                SELECT 1
                FROM hawkeye_customer hc 
                WHERE CONCAT(hc.hc_name, hc.hc_address, hc.hc_city, hc.hc_state) = t.OLD_KEY
            );
        ",

        // Remove the old columns from the CUSTOMERS and TRANSACTIONS tables
        "ALTER TABLE TRANSACTIONS DROP COLUMN OLD_KEY, DROP COLUMN OLD_TRANS_ID, 
            DROP COLUMN OLD_PROD_DESC",

        // Add a new column to the CUSTOMERS table to store the contact ID
        "ALTER TABLE CUSTOMERS ADD COLUMN CONTACT_ID INT REFERENCES CONTACTS(CONTACT_ID)",

        // update the CUSTOMERS table CONTACT_ID column to match the CONTACTS table
        "UPDATE CUSTOMERS SET CONTACT_ID = (SELECT CONTACT_ID FROM CONTACTS 
            WHERE CUSTOMERS.CUSTOMER_ID = CONTACTS.CUSTOMER_ID)",

        // also updates the CONTACT_ID column in the CUSTOMERS table, 
        // but only if the contact has a DOB
        // (only hawkeye contacts have a NOT NULL DOB)
        "UPDATE CUSTOMERS C SET C.CONTACT_ID = (SELECT C1.CONTACT_ID 
            FROM CONTACTS C1 WHERE CONCAT(C1.FIRST_NAME, ' ', C1.LAST_NAME) = C.CONTACT_NAME 
            AND C1.DOB IS NOT NULL ORDER BY C1.DOB DESC LIMIT 1) 
            WHERE EXISTS (SELECT 1 FROM CONTACTS C2 
                WHERE CONCAT(C2.FIRST_NAME, ' ', C2.LAST_NAME) = C.CONTACT_NAME 
                AND C2.DOB IS NOT NULL)",

        // Remove duplicate contacts from the CONTACTS table
        "DELETE FROM CONTACTS WHERE CONTACT_ID IN (SELECT MAX(CONTACT_ID) 
            FROM CONTACTS GROUP BY FIRST_NAME, LAST_NAME, PHONE HAVING COUNT(*) > 1)",

        // Remove unnecessary columns from the CUSTOMERS table
        "ALTER TABLE CUSTOMERS DROP COLUMN CONTACT_NAME, DROP COLUMN CONTACT_PHONE, 
            DROP COLUMN OLD_KEY",

        // Drop the old tables
        "DROP TABLE hawkeye_customer, hawkeye_product, hawkeye_transaction, hawkeye_contacts, 
                jewel_company, jewel_offerings, jewel_transactions;",

        // Rename all the new tables to match the hawk-eye naming convention
        "RENAME TABLE 
            CUSTOMERS TO hawkeye_customer, 
            TRANSACTIONS TO hawkeye_transactions, 
            PRODUCTS TO hawkeye_product, 
            CONTACTS TO hawkeye_contacts;",

        // Change all the names of the columns in each table to match the hawk-eye naming convention
        // This essentially replaces the old tables with the new ones, maintaining
        // compatibility with any existing queries or applications
        "ALTER TABLE hawkeye_customer 
            DROP PRIMARY KEY,
            CHANGE COLUMN CUSTOMER_ID hc_customer INT AUTO_INCREMENT PRIMARY KEY,
            CHANGE COLUMN NAME hc_name VARCHAR(37),
            CHANGE COLUMN ADDRESS hc_address VARCHAR(29),
            CHANGE COLUMN CITY hc_city VARCHAR(25),
            CHANGE COLUMN STATE hc_state CHAR(20),
            CHANGE COLUMN ZIP hc_zip INT(5),
            CHANGE COLUMN CONTACT_ID hc_contact INT,
            DROP COLUMN ACCT_OPEN_DATE",

        "ALTER TABLE hawkeye_product 
            DROP PRIMARY KEY,
            CHANGE COLUMN PRODUCT_ID product_id INT AUTO_INCREMENT PRIMARY KEY,
            CHANGE COLUMN PRODUCT_LINE product_line VARCHAR(17),
            CHANGE COLUMN CODE_NAME product_code_name VARCHAR(9),
            CHANGE COLUMN NOTES product_notes VARCHAR(2000)",

        "ALTER TABLE hawkeye_transactions
            DROP PRIMARY KEY,
            CHANGE COLUMN TRANSACTION_ID h_transaction_id INT AUTO_INCREMENT PRIMARY KEY,
            CHANGE COLUMN PRODUCT_ID h_product_id INT(4),
            CHANGE COLUMN CUSTOMER_ID h_customer_id INT,
            CHANGE COLUMN AMOUNT_PURCHASED h_amt INT(2),
            CHANGE COLUMN TRANSACTION_DATE h_purchase_date VARCHAR(10)",

        "ALTER TABLE hawkeye_contacts
            DROP PRIMARY KEY,
            CHANGE COLUMN CONTACT_ID hc_contact INT AUTO_INCREMENT PRIMARY KEY,
            CHANGE COLUMN FIRST_NAME hc_fname VARCHAR(20),
            CHANGE COLUMN LAST_NAME hc_lname VARCHAR(30),
            CHANGE COLUMN CUSTOMER_ID hc_customer INT,
            CHANGE COLUMN NOTES hc_notes VARCHAR(744),
            CHANGE COLUMN PHONE hc_phone VARCHAR(12),
            CHANGE COLUMN DOB hc_birthday VARCHAR(10);",

        // Changes states from abbreviations back to full names
        "UPDATE hawkeye_customer hc JOIN state_mapping sm ON hc.hc_state = sm.abbreviation 
            SET hc.hc_state = sm.FULL_NAME;",

        // Drop the state_mapping table because we don't need it anymore
        "DROP TABLE STATE_MAPPING;"
    ];

    $i = 0;

    // run each query and catch any errors
    try {
        foreach ($queries as $query) {
            if (mysqli_query($dbc, $query)) {
                echo "<p>Query (" . $queries[$i] . ") executed successfully</p>";
            } else {
                throw new Exception(mysqli_error($dbc));
            }
            $i++;
        }

        // only prints if all queries were successful
        // it won't make it to this point if any of the preceding queries fail
        echo "<h2><b>ALL FUNCTIONS COMPLETED SUCCESSFULLY</b></h2>";

    } catch (Exception $e) {
        echo "<p style='color:red';>Error executing Query " . 
            $queries[$i] . ":" . $e->getMessage() . "</p>";
    }
}

//*************** END FUNCTIONS ***************//


//*************** MAIN ***************//

// Pretty formatting stuff
echo "<style>
        h1 {
            margin-top: 1.5vh;
            text-align: center;
        }
        h2 {
            text-align: center;
        }
        p {
            text-align: center;
            margin-left: 1vw;
            margin-right: 1vw;
        }
        body {
            padding-top: 1vh;
            margin: auto 0;
            background-color: black;
            color: green;
            font-family: 'Courier New', monospace;
            font-size: 1.2em;
        }
        div {
            margin: auto;
            width: 30%;
            padding: 2vh 2vw;
            border: 2px solid green;
            border-radius: 10px;
        }
    </style>";

// print header
echo "<h1>Justin Becker - CIS267<br>Final Project</h1>";

// put everything in a nice, tasty div
echo "<div>";

CreateMergedCustomersTable($dbc);
CreateStateMappingTable($dbc);
InsertStateMappings($dbc);
MergeCustomerData($dbc);
CreateMergedTransactionsTable($dbc);
MergeTransactionData($dbc);
CreateMergedProductsTable($dbc);
MergeProductData($dbc);
CreateMergedContactsTable($dbc);
MergeContactData($dbc);
CleanUpTables($dbc);

echo "</div>";

// close the connection
mysqli_close($dbc);

//*************** END MAIN ***************//










