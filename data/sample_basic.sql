-- ==========================================
--    Mock Hive SQL queries (basic)
-- ==========================================
--
--     v0 : base
--     v1 : table alias
--     v2 : where
--     v3 : groupby
--     v4 : where + groupby
--     v5 : where + order by
--     v6 : 2 tables in from clause
--     v7 : 2 tables inner join
--     v8 : 2 tables outer join
--     v9 : case
--     v10: filling missing values
--     v11: subquery
--     v12: temp table
--     v13: drop

-- v0 (base)
create table schema.table as
select 
    col1,
    col2
from 
    schema_name.table_name_0;

-- v1 (table alias)
create table schema.table as
select 
    a.col1,
    a.col2
from 
    schema_name.table_name_1 as a;
  
-- v2 (where)
create table schema.table as
select 
    a.col1,
    a.col2
from 
    schema_name.table_name_2 as a  
where
    a.col2 > 15;

-- v3 (grouby)
create table schema.table as
select 
    a.col1,
    count(a.col1) as count_col1
from 
    schema_name.table_name_3 as a   
group by
    a.col1;

-- v4 (where + groupby)
create table schema.table as
select 
    a.col1,
    count(a.col1) as count_col1
from 
    schema_name.table_name_4 as a
where
    a.col1 > 15 
group by
    a.col1;

-- v5 (where + order by)
create table schema.table as
select 
    a.col1 AS col1_renamed,
    a.col2
from 
    schema_name.table_name_5 as a  
where
    a.col2 > 15
order by
    a.col1 desc;

-- v6 (2 tables in from clause)
create table schema.table as
select
    a.col1,
    b.col4
from
    schema_name.table_name_6_0 as a,
    schema_name.table_name_6_1 as b
where
    a.col1 = b.col1
    and a.col2 = b.col2
    and a.col3 = 15;
    
-- v7 (2 tables inner join)
create table schema.table as
select
    a.col1,
    b.col4
from
    schema_name.table_name_7_0 as a
JOIN
    schema_name.table_name_7_1 as b
ON
    a.col1 = b.col1
    and a.col2 = b.col2
    and a.col3 = 15;

-- v8 (2 tables outer join)
create table schema.table as
select
    a.col1,
    b.col4
from
    schema_name.table_name_8_0 as a
OUTER JOIN
    schema_name.table_name_8_1 as b
ON
    a.col1 = b.col1
    and a.col2 = b.col2
    and a.col3 = 15;

-- v9 (case)
create table schema.table as
select
    a.col1,
    a.col2 AS col2_renamed,
    CASE
        WHEN a.col3 = 15
        THNE 1
        ELSE 0
    END AS col3_ind
from
    schema_name.table_name_9 as a;

-- v10 (filling missing value)
create table schema.table as
select
    a.col1,
    COALESCE(a.col2, 0) as col2_coalesce,
    NVL(a.col3, 0) as col3_nvl
from
    schema_name.table_name_10 as a;

-- v11 (sub query)
create table schema.table as
select
    a.col1,
    b.col4,
from
(
    select
        c.col1
    from
        schema_name_2.table_name_11_0 as c
) AS a
JOIN
    schema_name.table_name_11_2 as b
ON
    a.col1 = b.col1
    and a.col2 = b.col2
    and a.col3 = 15;

-- v12 (temp table)
create table schema.table as
with
tmp_table as
(
    select
        c.col1
    from
        schema_name_2.table_name_12_0 AS c
)
select
    a.col1,
    b.col4,
    CASE
        WHEN b.col5 = a.col7
        THNE 1
        ELSE 0
    END AS col5,
    COALESCE(b.col6, 0) as col8
from
    tmp_table as a
JOIN
    schema_name.table_name_12_1 as b
ON
    a.col1 = b.col1;

-- v13 (drop table)
drop table if exists schema_name.table_name_13;