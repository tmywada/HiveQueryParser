-- ==========================================
--    Mock Hive SQL queries (temp tables)
-- ==========================================
--
create table schema.table as
with
tmp_table_0 as
(
    select
        a.col1,
        a.col2,
        a.col3,
        a.col4
    from
        schema_name_2.table_name_source0 AS a
)
select
    A.col1,
    A.col2,
    A.col3,
    B.col4,
    CASE
        WHEN b.col5 = a.col4
        THNE 1
        ELSE 0
    END AS col5,
    COALESCE(b.col6, 0) as col6_fill_na
from
    tmp_table_0 as A
where
    a.col1 = b.col1;