DROP TABLE IF EXISTS Avg_Retail_Data;
CREATE TABLE IF NOT EXISTS Avg_Retail_Data (

    ItemName VARCHAR(255), 
    ItemColor VARCHAR(255),
    avg_price FLOAT,
    total_quantity INTEGER,
    RevenueItem INTEGER,
    total INTEGER,
    ts TIMESTAMP 

);
