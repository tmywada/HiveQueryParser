-- ==========================================
--    Mock Hive SQL queries (having)
-- ==========================================
--
create table schema.table as
SELECT 
    a.col1, 
    a.col2, 
    COUNT(DISTINCT a.col3) AS col3_sum_distinct,
    COUNT(a.col3) AS col3_sum
FROM
    schmea_name.table_name AS a
WHERE
    a.col3 IN (1,2)
GROUP BY 
    a.col1, a.col2
HAVING
    COUNT(DISTINCT a.col3) > 1
    or a.col3 == -10
