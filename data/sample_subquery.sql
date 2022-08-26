-- ==========================================
--    Mock Hive SQL queries (sub-query)
-- ==========================================
--
create table schema.table as
select
    a.col1,
    a.col2,
    b.col4
from
(
    select
        c.col1,
        c.col2
    from
    (
        select
            d.col1,
            d.col2
        from
            schema_name_2.table_name_11_0 as d
    ) AS c      
) AS a
JOIN
    schema_name.table_name_subquery as b
ON
    a.col1 = b.col1;
