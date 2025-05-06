-- =========================================
-- ETL PROJECT: Cleaning and Transforming E-commerce Order Data using MySQL
-- =========================================
-- STEP 1: Extract - Simulate raw data load into 'messy_data' table
-- =========================================

CREATE TABLE messy_data (
    order_id INT,
    customer_name VARCHAR(255),
    order_date VARCHAR(50),  -- Stored as text due to inconsistent formats
    product VARCHAR(255),
    quantity VARCHAR(10),    -- Stored as text due to possible non-numeric entries
    price VARCHAR(10),       -- Stored as text due to presence of invalid values
    PRIMARY KEY (order_id)
);

-- Insert raw and inconsistent data into messy_data (simulating raw extract)
INSERT INTO messy_data (order_id, customer_name, order_date, product, quantity, price) VALUES
(1, 'John Doe', '2023-12-01', 'Widget A', '5', '19.99'),
(2, 'Jane Smith', '23/11/2023', 'Widget B', '2', '29.99'),
(3, 'John Doe', '2023-12-01', 'Widget A', '5', '19.99'),
(4, NULL, '15-10-2023', 'Widget C', '3', '15.50'),
(5, 'Alice Brown', '2023/12/05', 'Widget D', '0', '25.00'),
(6, 'Bob Wilson', '2023-13-01', 'Widget E', '-2', '30.00'),
(7, 'Charlie', '2023-11-30', 'Widget F', NULL, '40.00'),
(8, 'Eve Adams', '2023-11-29', 'Widget G', '4', 'abc');

-- =========================================
-- STEP 2: Transform - Clean and Standardize the Data
-- =========================================

-- Fix invalid price entries (non-numeric values like 'abc')
UPDATE messy_data
SET price = 0
WHERE price = 'abc';

-- Clean quantity column: convert NULLs to 0, remove negatives
UPDATE messy_data
SET quantity =
    CASE
        WHEN quantity IS NULL THEN 0
        ELSE ABS(quantity)
    END;

-- Enforce data integrity: ensure quantity is a non-null positive integer
ALTER TABLE messy_data
MODIFY COLUMN quantity INT CHECK (quantity >= 0);

ALTER TABLE messy_data
MODIFY COLUMN quantity INT NOT NULL;

-- Normalize date format by replacing '/' with '-' for consistency
UPDATE messy_data
SET order_date = REPLACE(order_date, '/', '-');

-- Convert order_date to correct date format (DD-MM-YYYY to YYYY-MM-DD)
UPDATE messy_data
SET order_date = 
  CASE 
    WHEN order_date LIKE '__-__-____' THEN STR_TO_DATE(order_date, '%d-%m-%Y')
    ELSE order_date
  END;

-- Fix invalid date (month 13) manually for specific order_id
UPDATE messy_data
SET order_date = '2023-01-13'
WHERE order_id = 6;

-- Convert order_date column to actual DATE type
ALTER TABLE messy_data
MODIFY COLUMN order_date DATE;

-- Handle missing customer names by replacing NULLs with placeholder
UPDATE messy_data
SET customer_name = 'Not found'
WHERE customer_name IS NULL;

-- Identify duplicate customer entries
SELECT
    customer_name
FROM
    messy_data
GROUP BY customer_name
HAVING COUNT(*) > 1;

-- Remove exact duplicate record (e.g., repeated order for John Doe)
DELETE FROM messy_data
WHERE order_id = 3;

-- Convert price column to numeric type (DECIMAL)
ALTER TABLE messy_data
MODIFY COLUMN price DECIMAL(10,2);

-- Set order_id to auto-increment for future insertions
ALTER TABLE messy_data
MODIFY COLUMN order_id INT AUTO_INCREMENT;

-- =========================================
-- STEP 3: Load - Move Cleaned Data to Final Destination Table
-- =========================================

-- Create clean_data table with proper schema and constraints
CREATE TABLE clean_data (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    order_date DATE,
    product VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DOUBLE NOT NULL
);

-- Insert cleaned and transformed data into clean_data
INSERT INTO clean_data (order_id, customer_name, order_date, product, quantity, price)
SELECT * FROM messy_data;

-- =========================================
-- ETL Process Complete: Data is now clean and ready for reporting/analysis
-- =========================================
