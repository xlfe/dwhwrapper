

CREATE MULTISET TABLE dwhwrapper_Test_Table AS
(
    SELECT
            CAST(9.0 as DECIMAL(1,0)) as DEC10
        ,   CAST(1.3 as DECIMAL(2,1)) as DEC21a
        ,   CAST(9.9 as DECIMAL(2,1)) as DEC21b
        ,   CAST(99.9 as DECIMAL(3,1)) as DEC31
        ,   CAST(99.99 as DECIMAL(4,2)) as DEC42
        ,   CAST(999.99 as DECIMAL(5,2)) as DEC52
        ,   CAST(9999.99 as DECIMAL(6,2)) as DEC62
        ,   CAST(99999.99 as DECIMAL(7,2)) as DEC72
        ,   CAST(999999.99 as DECIMAL(8,2)) as DEC82
        ,   CAST(9999999.99 as DECIMAL(9,2)) as DEC92
        ,   CAST(99999999.99 as DECIMAL(10,2)) as DEC102
        ,   CAST(999999999.99 as DECIMAL(11,2)) as DEC112
        ,   CAST(9999999999.99 as DECIMAL(12,2)) as DEC122
        ,   CAST(99999999999.99 as DECIMAL(13,2)) as DEC132
        ,   CAST(999999999999.99 as DECIMAL(14,2)) as DEC142
        ,   CAST(9999999999999.99 as DECIMAL(15,2)) as DEC152
        ,   CAST(99999999999999.99 as DECIMAL(16,2)) as DEC162
        ,   CAST(999999999999999.99 as DECIMAL(17,2)) as DEC172
        ,   CAST(9999999999999999.99 as DECIMAL(18,2)) as DEC182
        ,   CAST(99999999999999999.9 as DECIMAL(18,1)) as DEC181
        ,   CAST(-99999999999999.9999 as DECIMAL(18,4)) as DECm184
        ,   CAST(-1999999999999.9999 as DECIMAL(17,4)) as DECm174
        ,   CAST(-99999.9999 as DECIMAL(9,4)) as DECm94
        ,   CAST(-99.9 as DECIMAL(4,1)) as DECm31
        ,   DATE'2000-01-01' as DT20
        ,   CAST(NULL as FLOAT) as FLN
        ,   DATE'1900-01-01' as DT19
        ,   DATE'2500-01-01' as DT25
        ,   DATE'1980-12-31' as DT80
        ,   CAST('6chars' as VARCHAR(39)) as VC6
        ,   CAST('14 characters ' as VARCHAR(14)) as VC14
        ,   CAST('          ' as VARCHAR(16)) as VC10
        ,   CAST('space at the end of this' as CHAR(24)) as C24
        ,   CAST(1234522121.12 as FLOAT) as FL1
        ,   CAST(522121.12345 as FLOAT) as FL2
        ,   CAST(6.5 as FLOAT) as FL0
        ,   CAST(123237832 AS INTEGER) as I1
        ,   CAST(6554 AS SMALLINT) as SI1
        ,   CAST(-6554 AS SMALLINT) as mSI1
        ,   CAST(-100 AS BYTEINT) as mBI1
        ,   CAST(-.2 as DECIMAL(5,2)) as nD1
        ,   CAST(-.000003 as DECIMAL(8,6)) as nD2
        ,   CAST(NULL as DECIMAL) as NLD
        ,   CAST('A' AS CHAR(1)) as CH1
        ,   CAST('2011-12-25 12:04:02' as TIMESTAMP) as TS1
        ,   CAST('11/12/25' as DATE FORMAT 'YY/MM/DD') as DF1
        ,   CAST(120 AS BYTEINT) as BI2       
)WITH DATA;


--select * from dwhwrapper_test_table
