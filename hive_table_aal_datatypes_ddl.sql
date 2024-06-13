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
