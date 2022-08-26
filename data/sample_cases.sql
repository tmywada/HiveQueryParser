-- ==========================================
--    Mock Hive SQL queries (cases)
-- ==========================================
--
create table schema.table as
select
    a.col1,
    CASE
        WHEN a.col2 = 15
        THEN 1
        ELSE
        	CASE
        		WHEN a.col2 < 0
        		THEN 2
        		ELSE
        			CASE
        				WHEN a.col2 > 15
                             AND a.col1 = a.col3
        				THEN 3
        				ELSE 0
        			END
    		END
    END AS col2_ind
from
    schema_name.table_name_case as a;
