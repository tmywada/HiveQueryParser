-- ==========================================
--    Mock Hive SQL queries (case)
-- ==========================================
--
create table schema.table as
select
    a.col1,
    CASE
        WHEN a.col2 = 15
        THNE 1
        ELSE
        	CASE
        		WHEN a.col2 < 0:
        		THEN 2
        		ELSE
        			CASE
        				WHEN a.col2 > 15
        				THEN 3
        				ELSE 0
        			END
    		END
    END AS col2_ind
from
    schema_name.table_name_case as a;
