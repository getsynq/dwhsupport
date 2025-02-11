WITH rows_estimation AS (select "database",
                                "schema",
                                "table",
                                estimated_visible_rows as "row_count"
                         from pg_catalog.svv_table_info
                         where estimated_visible_rows is not null
                           AND schema <> 'catalog_history'::name
                           AND schema <> 'pg_toast'::name
                           AND schema <> 'pg_internal'::name
                           AND database = $1),
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
