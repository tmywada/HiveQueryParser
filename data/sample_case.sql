-- ==========================================
--    Mock Hive SQL queries (case)
-- ==========================================
--
create table schema.table as
select
    CASE
        WHEN a.col2 = 15
             and a.col3 < a.col1
        THEN 1
        ELSE Null
    END AS col2_ind
from
    schema_name.table_name_case as a;
