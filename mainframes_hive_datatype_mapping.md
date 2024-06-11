### Mainframe Copybook Data Types and Corresponding Hive Data Types

1. **Alphanumeric (PIC X(n))**
   - **Mainframe:** PIC X(n)
   - **Hive:** STRING

2. **Numeric (Packed Decimal - COMP-3)**
   - **Mainframe:** PIC 9(n)V9(m) COMP-3
   - **Hive:** DECIMAL(precision, scale)

3. **Numeric (Binary - COMP)**
   - **Mainframe:** PIC 9(n) COMP
   - **Hive:** INT/BIGINT (depending on the length)

4. **Numeric (Binary - COMP-4)**
   - **Mainframe:** PIC 9(n) COMP-4
   - **Hive:** INT/BIGINT (depending on the length)

5. **Numeric (Binary - COMP-5)**
   - **Mainframe:** PIC 9(n) COMP-5
   - **Hive:** INT/BIGINT (depending on the length)

6. **Numeric (Display)**
   - **Mainframe:** PIC 9(n)V9(m)
   - **Hive:** DECIMAL(precision, scale)

7. **Numeric (Floating Point - COMP-1)**
   - **Mainframe:** COMP-1 (single-precision floating point)
   - **Hive:** FLOAT

8. **Numeric (Floating Point - COMP-2)**
   - **Mainframe:** COMP-2 (double-precision floating point)
   - **Hive:** DOUBLE

9. **Signed Numeric (Display)**
   - **Mainframe:** PIC S9(n)V9(m) 
   - **Hive:** DECIMAL(precision, scale)

10. **Signed Numeric (Packed Decimal - COMP-3)**
    - **Mainframe:** PIC S9(n)V9(m) COMP-3
    - **Hive:** DECIMAL(precision, scale)

11. **Signed Numeric (Binary - COMP)**
    - **Mainframe:** PIC S9(n) COMP
    - **Hive:** INT/BIGINT (depending on the length)

12. **Signed Numeric (Binary - COMP-4)**
    - **Mainframe:** PIC S9(n) COMP-4
    - **Hive:** INT/BIGINT (depending on the length)

13. **Signed Numeric (Binary - COMP-5)**
    - **Mainframe:** PIC S9(n) COMP-5
    - **Hive:** INT/BIGINT (depending on the length)

14. **Unsigned Numeric (Display)**
    - **Mainframe:** PIC 9(n)
    - **Hive:** INT/BIGINT/DECIMAL (depending on the length and precision required)



### Key Notes

- **STRING**: Used for alphanumeric data (PIC X(n)).
- **DECIMAL(precision, scale)**: Used for numeric data with a specified number of digits and decimal places.
- **INT/BIGINT**: Used for numeric data, with INT for shorter lengths and BIGINT for longer lengths.
- **FLOAT/DOUBLE**: Used for floating-point numbers.

### Additional Considerations

- **Handling Null Values:** Mainframe systems often use special values to indicate nulls. These should be appropriately mapped to NULL in Hive.
- **Field Lengths:** Ensure that the lengths of fields in Hive match those in the mainframe copybooks.
- **Data Formatting:** Be aware of any specific formatting requirements or conversions needed between EBCDIC (mainframe) and ASCII (Hive).

```
- Copybook
       01  TEST-DATA.
           05  ALPHANUMERIC-FIELD          PIC X(10).
           05  PACKED-DECIMAL-FIELD        PIC 9(5)V9(2) COMP-3.
           05  BINARY-FIELD                PIC 9(5) COMP.
           05  BINARY-FIELD-COMP4          PIC 9(5) COMP-4.
           05  BINARY-FIELD-COMP5          PIC 9(5) COMP-5.
           05  DISPLAY-NUMERIC-FIELD       PIC 9(5)V9(2).
           05  FLOAT-FIELD                 COMP-1.
           05  DOUBLE-FIELD                COMP-2.
           05  SIGNED-DISPLAY-FIELD        PIC S9(5)V9(2).
           05  SIGNED-PACKED-DECIMAL       PIC S9(5)V9(2) COMP-3.
           05  SIGNED-BINARY-FIELD         PIC S9(5) COMP.
           05  SIGNED-BINARY-FIELD-COMP4   PIC S9(5) COMP-4.
           05  SIGNED-BINARY-FIELD-COMP5   PIC S9(5) COMP-5.
           05  UNSIGNED-DISPLAY-FIELD      PIC 9(10).

- Data
ALPHA12345
+1234500
00012345
00012345
00012345
0001234500
4.567
1234567890.1234
-1234500
-1234500
-12345
-12345
-12345
1234567890

- DDL
CREATE TABLE test_data (
  alphanumeric_field STRING,
  packed_decimal_field DECIMAL(7,2),
  binary_field INT,
  binary_field_comp4 INT,
  binary_field_comp5 INT,
  display_numeric_field DECIMAL(7,2),
  float_field FLOAT,
  double_field DOUBLE,
  signed_display_field DECIMAL(7,2),
  signed_packed_decimal DECIMAL(7,2),
  signed_binary_field INT,
  signed_binary_field_comp4 INT,
  signed_binary_field_comp5 INT,
  unsigned_display_field BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n';
```
