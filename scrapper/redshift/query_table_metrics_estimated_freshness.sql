WITH rows_estimation AS (select "database",
                                "schema",
                                "table",
                                estimated_visible_rows as "row_count",
                                -- size is in 1 MB blocks
                                size::bigint * 1048576 as "size_bytes"
                         from pg_catalog.svv_table_info
                         where estimated_visible_rows is not null
                           AND schema not in ({{system_schemas}})
                           AND database = $1
                           /* SYNQ_SCOPE_FILTER */),
     freshness_estimation AS (SELECT ti.database,
                                     ti.schema,
                                     ti.table,
                                     MAX(end_time) as updated_at
                              FROM sys_query_detail qd
                                       JOIN svv_table_info ti USING (table_id)
                              WHERE table_id > 0
                                AND step_name IN ('insert', 'delete')
                                AND end_time >= dateadd('day', -2, current_date)
                                AND end_time IS NOT NULL
                                AND ti.database = $1
                              GROUP BY ti.database, ti.schema, ti.table)

SELECT *
FROM rows_estimation
         LEFT JOIN freshness_estimation USING (database, schema, "table")
