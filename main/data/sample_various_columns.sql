-- ==========================================
--    Mock Hive SQL queries (various columns)
-- ==========================================
--
create table schema.table as
select
    a.col1,
    COALESCE(a.col2, 0) as col2_coalesce,
    NVL(a.col3, 0) as col3_nvl    
    CASE
        WHEN a.col3 == b.col3
        THNE 1
        ELSE 0
    END AS col3_comaprison_ind,
    b.col4 AS col4_renamed,
    count(b.col5) AS col5_sum,
    1 AS col6,
    b.col7 - b.col6 AS col7,
    1.1 AS col8,
    'A' AS col9,
    cast(b.col10 AS bigint) AS col10_bigint,
    col11
from
    schema_name.table_name_various as a
join
    schema_name.table_name_various as b
on
    a.col1 == b.col2
    and a.col2 > b.col2;
