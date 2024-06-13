CREATE TABLE IF NOT EXISTS all_data_types (
    tinyint_col     TINYINT,
    smallint_col    SMALLINT,
    int_col         INT,
    bigint_col      BIGINT,
    boolean_col     BOOLEAN,
    float_col       FLOAT,
    double_col      DOUBLE,
    string_col      STRING,
    varchar_col     VARCHAR(255), -- Max length of 255
    char_col        CHAR(10),     -- Fixed length of 10
    timestamp_col   TIMESTAMP,
    date_col        DATE,
    decimal_col     DECIMAL(10,2), -- Precision 10, scale 2
    binary_col      BINARY,
    array_col       ARRAY<STRING>,
    map_col         MAP<STRING, INT>,
    struct_col      STRUCT<name:STRING, age:INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

Data
120,32000,1234567890,-9223372036854775800,true,3.14159,2.71828,"This is a string","Sample varchar text","Char value",2024-06-13 10:30:00,2023-12-25,12345.67,01010100,["apple","banana","cherry"],{"key1":10,"key2":25},{"name":"Alice","age":30}
-100,-15000,-543210987,9223372036854775807,false,-10.5,0.618,"Another string here","More varchar","Char#2",2022-01-01 00:00:00,2024-06-13,8765.43,10101011,["red","green","blue"],{"key3":5,"key4":12},{"name":"Bob","age":45}
