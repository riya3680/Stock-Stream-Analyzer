create database kafka;
use kafka;

CREATE TABLE stock_data (
    ticker VARCHAR(10),
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume INT,
    timestamps TIMESTAMP,
    currenttime TIMESTAMP
);

ALTER TABLE stock_data
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

desc stock_data;

select * from stock_data;




