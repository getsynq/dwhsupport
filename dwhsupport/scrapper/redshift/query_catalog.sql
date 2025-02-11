with svv_redshift_columns as ((SELECT current_database()::character varying(128)                            AS database_name,
                                      pns.nspname::character varying(128)                                   AS schema_name,
                                      pgc.relname::character varying(128)                                   AS table_name,
                                      pgc.relkind = 'v'::"char"                                             AS is_view,
                                      pga.attname::character varying(128)                                   AS column_name,
                                      pga.attnum                                                            AS ordinal_position,
                                      format_type(pga.atttypid, pga.atttypmod)::character varying           AS data_type,
                                      ad.adsrc::character varying(4000)                                     AS column_default,
                                      CASE
                                          WHEN pga.attnotnull THEN 'NO'::text
                                          ELSE 'YES'::text
                                          END::character varying(3)                                         AS is_nullable,
                                      format_encoding(pga.attencodingtype::integer)::character varying(128) AS "encoding",
                                      pga.attisdistkey                                                      AS "distkey",
                                      pga.attsortkeyord                                                     AS "sortkey",
                                      ''::character varying(128)                                            AS column_acl,
                                      d.description::character varying(256)                                 AS remarks
                               FROM pg_attribute pga
                                        JOIN pg_class pgc ON pga.attrelid = pgc.oid
                                        JOIN pg_namespace pns ON pns.oid = pgc.relnamespace
                                        LEFT JOIN pg_attrdef ad ON pga.attrelid = ad.adrelid AND pga.attnum = ad.adnum
                                        LEFT JOIN pg_description d ON pgc.oid = d.objoid AND pga.attnum = d.objsubid
                               WHERE pga.attnum > 0
                                 AND NOT pga.attisdropped
                                 AND (pgc.relkind = 'r'::"char" OR pgc.relkind = 'v'::"char" OR
                                      pgc.relkind = 'm'::"char")
                                 AND pns.nspname <> 'catalog_history'::name
                                 AND pns.nspname <> 'pg_automv'::name
                                 AND pns.nspname <> 'pg_toast'::name
                                 AND pns.nspname <> 'pg_internal'::name
                                 AND pns.nspname !~~ 'pg_temp%'::text
                               UNION ALL
                               SELECT current_database()::character varying(128)     AS database_name,
                                      lbv_cols."schema"::character varying(128)      AS schema_name,
                                      lbv_cols.viewname::character varying(128)      AS table_name,
                                      1::bool                                        AS is_view,
                                      lbv_cols.colname::character varying(128)       AS column_name,
                                      lbv_cols.colnum                                AS ordinal_position,
                                      lbv_cols."type"::character varying(128)        AS data_type,
                                      ''::character varying::character varying(4000) AS column_default,
                                      'YES'::character varying::character varying(3) AS is_nullable,
                                      ''                                             AS "encoding",
                                      'f'                                            AS "distkey",
                                      0                                              AS "sortkey",
                                      ''                                             AS column_acl,
                                      ''                                             AS remarks
                               FROM pg_get_late_binding_view_cols() lbv_cols("schema" name, viewname name, colname name,
                                                                             "type" character varying,
                                                                             colnum integer)
                                        JOIN pg_class pgc ON pgc.relname = lbv_cols.viewname
                                        JOIN pg_namespace pns ON lbv_cols."schema" = pns.nspname
                               WHERE pns.oid = pgc.relnamespace
                                 AND lbv_cols."schema" <> 'catalog_history'::name
                                 AND lbv_cols."schema" <> 'pg_internal'::name
                                 AND lbv_cols."schema" <> 'pg_automv'::name
                                 AND lbv_cols."schema" <> 'pg_toast'::name
                                 AND lbv_cols."schema" !~~ 'pg_temp%'::text)
                              UNION ALL
                              SELECT btrim(rs_cols.database_name::text)::character varying(128)   AS database_name,
                                     btrim(rs_cols.schema_name::text)::character varying(128)     AS schema_name,
                                     btrim(rs_cols.table_name::text)::character varying(128)      AS table_name,
                                     0::bool                                                      AS is_view,
                                     btrim(rs_cols.column_name::text)::character varying(128)     AS column_name,
                                     rs_cols.column_number                                        AS ordinal_position,
                                     btrim(rs_cols.data_type::text)::character varying(128)       AS data_type,
                                     btrim(rs_cols.column_default::text)::character varying(4000) AS column_default,
                                     CASE
                                         WHEN rs_cols.is_nullable THEN 'YES'::text
                                         ELSE 'NO'::text
                                         END::character varying(3)                                AS is_nullable,
                                     btrim(rs_cols."compression"::text)::character varying(128)   AS "encoding",
                                     rs_cols.is_dist_key                                          AS "distkey",
                                     rs_cols.sort_key                                             AS "sortkey",
                                     btrim(rs_cols.column_acl::text)::character varying(128)      AS column_acl,
                                     btrim(rs_cols.remarks::text)::character varying(256)         AS remarks
                              FROM pg_get_shared_redshift_columns() rs_cols(database_name character varying,
                                                                            schema_name character varying,
                                                                            table_name character varying,
                                                                            column_name character varying,
                                                                            column_number integer,
                                                                            data_type character varying,
                                                                            column_default character varying,
                                                                            is_nullable boolean,
                                                                            "compression" character varying,
                                                                            is_dist_key boolean, sort_key integer,
                                                                            column_acl character varying,
                                                                            remarks character varying)),
     svv_all_columns as (((SELECT svv_redshift_columns.database_name,
                                  svv_redshift_columns.schema_name,
                                  svv_redshift_columns.table_name,
                                  svv_redshift_columns.is_view,
                                  svv_redshift_columns.column_name,
                                  svv_redshift_columns.ordinal_position,
                                  svv_redshift_columns.column_default,
                                  svv_redshift_columns.is_nullable,
                                  CASE
                                      WHEN "left"(svv_redshift_columns.data_type::text, 7) = 'numeric'::text OR
                                           "left"(svv_redshift_columns.data_type::text, 7) = 'decimal'::text
                                          THEN 'numeric'::character varying
                                      WHEN "left"(svv_redshift_columns.data_type::text, 7) = 'varchar'::text OR
                                           "left"(svv_redshift_columns.data_type::text, 17) = 'character varying'::text
                                          THEN 'character varying'::character varying
                                      WHEN "left"(svv_redshift_columns.data_type::text, 7) = 'varbyte'::text OR
                                           "left"(svv_redshift_columns.data_type::text, 14) = 'binary varying'::text
                                          THEN 'binary varying'::character varying
                                      WHEN "left"(svv_redshift_columns.data_type::text, 4) = 'char'::text OR
                                           "left"(svv_redshift_columns.data_type::text, 9) = 'character'::text
                                          THEN 'character'::character varying
                                      WHEN svv_redshift_columns.data_type::text = 'information_schema.sql_identifier'::text
                                          THEN 'sql_identifier'::character varying
                                      WHEN svv_redshift_columns.data_type::text = 'information_schema.character_data'::text
                                          THEN 'character_data'::character varying
                                      WHEN svv_redshift_columns.data_type::text = 'information_schema.cardinal_number'::text
                                          THEN 'cardinal_number'::character varying
                                      ELSE svv_redshift_columns.data_type
                                      END::character varying(128) AS data_type,
                                  svv_redshift_columns.remarks
                           FROM svv_redshift_columns
                           UNION ALL
                           SELECT btrim(ext_columns.redshift_database_name::text)::character varying(128) AS database_name,
                                  btrim(ext_columns.schemaname::text)::character varying(128)             AS schema_name,
                                  btrim(ext_columns.tablename::text)::character varying(128)              AS table_name,
                                  0::bool                                                                 AS is_view,
                                  btrim(ext_columns.columnname::text)::character varying(128)             AS column_name,
                                  ext_columns.columnnum                                                   AS ordinal_position,
                                  NULL::"unknown"                                                         AS column_default,
                                  CASE
                                      WHEN ext_columns.is_nullable::text = 'true'::text THEN 'YES'::text
                                      WHEN ext_columns.is_nullable::text = 'false'::text THEN 'NO'::text
                                      ELSE ''::text
                                      END::character varying(3)                                           AS is_nullable,
                                  CASE
                                      WHEN "left"(ext_columns.external_type::text, 7) = 'varchar'::text OR
                                           "left"(ext_columns.external_type::text, 17) = 'character varying'::text
                                          THEN 'character varying'::character varying
                                      WHEN "left"(ext_columns.external_type::text, 7) = 'varbyte'::text OR
                                           "left"(ext_columns.external_type::text, 14) = 'binary varying'::text
                                          THEN 'binary varying'::character varying
                                      WHEN "left"(ext_columns.external_type::text, 4) = 'char'::text
                                          THEN 'character'::character varying
                                      WHEN "left"(ext_columns.external_type::text, 7) = 'decimal'::text
                                          THEN 'numeric'::character varying
                                      WHEN "left"(ext_columns.external_type::text, 7) = 'numeric'::text
                                          THEN 'numeric'::character varying
                                      WHEN ext_columns.external_type::text = 'float'::text
                                          THEN 'real'::character varying
                                      WHEN ext_columns.external_type::text = 'double'::text
                                          THEN 'double precision'::character varying
                                      WHEN ext_columns.external_type::text = 'int'::text OR
                                           ext_columns.external_type::text = 'int4'::text
                                          THEN 'integer'::character varying
                                      WHEN ext_columns.external_type::text = 'int2'::text
                                          THEN 'smallint'::character varying
                                      ELSE ext_columns.external_type
                                      END::character varying(128)                                         AS data_type,
                                  NULL::"unknown"                                                         AS remarks
                           FROM pg_get_external_columns() ext_columns(es_or_edb_oid integer,
                                                                      redshift_database_name character varying,
                                                                      schemaname character varying,
                                                                      tablename character varying,
                                                                      columnname character varying,
                                                                      external_type character varying,
                                                                      columnnum integer, part_key integer,
                                                                      is_nullable character varying))
                          UNION ALL
                          SELECT btrim(ext_columns.redshift_database_name::text)::character varying(128) AS database_name,
                                 btrim(ext_columns.schemaname::text)::character varying(128)             AS schema_name,
                                 btrim(ext_columns.tablename::text)::character varying(128)              AS table_name,
                                 0::bool                                                                 AS is_view,
                                 btrim(ext_columns.columnname::text)::character varying(128)             AS column_name,
                                 ext_columns.columnnum                                                   AS ordinal_position,
                                 NULL::"unknown"                                                         AS column_default,
                                 CASE
                                     WHEN ext_columns.is_nullable::text = 'true'::text THEN 'YES'::text
                                     WHEN ext_columns.is_nullable::text = 'false'::text THEN 'NO'::text
                                     ELSE ''::text
                                     END::character varying(3)                                           AS is_nullable,
                                 CASE
                                     WHEN "left"(ext_columns.external_type::text, 7) = 'varchar'::text OR
                                          "left"(ext_columns.external_type::text, 17) = 'character varying'::text
                                         THEN 'character varying'::character varying
                                     WHEN "left"(ext_columns.external_type::text, 7) = 'varbyte'::text OR
                                          "left"(ext_columns.external_type::text, 14) = 'binary varying'::text
                                         THEN 'binary varying'::character varying
                                     WHEN "left"(ext_columns.external_type::text, 4) = 'char'::text
                                         THEN 'character'::character varying
                                     WHEN "left"(ext_columns.external_type::text, 7) = 'decimal'::text
                                         THEN 'numeric'::character varying
                                     WHEN "left"(ext_columns.external_type::text, 7) = 'numeric'::text
                                         THEN 'numeric'::character varying
                                     WHEN ext_columns.external_type::text = 'float'::text THEN 'real'::character varying
                                     WHEN ext_columns.external_type::text = 'double'::text
                                         THEN 'double precision'::character varying
                                     WHEN ext_columns.external_type::text = 'int'::text OR
                                          ext_columns.external_type::text = 'int4'::text
                                         THEN 'integer'::character varying
                                     WHEN ext_columns.external_type::text = 'int2'::text
                                         THEN 'smallint'::character varying
                                     ELSE ext_columns.external_type
                                     END::character varying(128)                                         AS data_type,
                                 NULL::"unknown"                                                         AS remarks
                          FROM pg_get_external_database_columns() ext_columns(es_or_edb_oid integer,
                                                                              redshift_database_name character varying,
                                                                              schemaname character varying,
                                                                              tablename character varying,
                                                                              columnname character varying,
                                                                              external_type character varying,
                                                                              columnnum integer, part_key integer,
                                                                              is_nullable character varying))
                         UNION ALL
                         SELECT btrim(ext_columns.databasename::text)::character varying(128) AS database_name,
                                btrim(ext_columns.schemaname::text)::character varying(128)   AS schema_name,
                                btrim(ext_columns.tablename::text)::character varying(128)    AS table_name,
                                0::bool                                                       AS is_view,
                                btrim(ext_columns.columnname::text)::character varying(128)   AS column_name,
                                ext_columns.columnnum                                         AS ordinal_position,
                                NULL::"unknown"                                               AS column_default,
                                CASE
                                    WHEN ext_columns.is_nullable::text = 'true'::text THEN 'YES'::text
                                    WHEN ext_columns.is_nullable::text = 'false'::text THEN 'NO'::text
                                    ELSE ''::text
                                    END::character varying(3)                                 AS is_nullable,
                                CASE
                                    WHEN "left"(ext_columns.external_type::text, 7) = 'varchar'::text OR
                                         "left"(ext_columns.external_type::text, 17) = 'character varying'::text
                                        THEN 'character varying'::character varying
                                    WHEN "left"(ext_columns.external_type::text, 7) = 'varbyte'::text OR
                                         "left"(ext_columns.external_type::text, 14) = 'binary varying'::text
                                        THEN 'binary varying'::character varying
                                    WHEN "left"(ext_columns.external_type::text, 4) = 'char'::text
                                        THEN 'character'::character varying
                                    WHEN "left"(ext_columns.external_type::text, 7) = 'decimal'::text
                                        THEN 'numeric'::character varying
                                    WHEN "left"(ext_columns.external_type::text, 7) = 'numeric'::text
                                        THEN 'numeric'::character varying
                                    WHEN ext_columns.external_type::text = 'float'::text THEN 'real'::character varying
                                    WHEN ext_columns.external_type::text = 'double'::text
                                        THEN 'double precision'::character varying
                                    WHEN ext_columns.external_type::text = 'int'::text OR
                                         ext_columns.external_type::text = 'int4'::text
                                        THEN 'integer'::character varying
                                    WHEN ext_columns.external_type::text = 'int2'::text
                                        THEN 'smallint'::character varying
                                    ELSE ext_columns.external_type
                                    END::character varying(128)                               AS data_type,
                                NULL::"unknown"                                               AS remarks
                         FROM pg_get_all_external_columns() ext_columns(databasename character varying,
                                                                        schemaname character varying,
                                                                        tablename character varying, esoid integer,
                                                                        columnname character varying,
                                                                        external_type character varying,
                                                                        columnnum integer, part_key integer,
                                                                        is_nullable character varying))
select database_name         as database,
       schema_name           as schema,
       table_name            as table,
       is_view               as is_view,
       column_name           as column,
       data_type             as type,
       ordinal_position      as position,
       lower(type) = 'super' as is_struct_column,
       lower(type) = 'super' as is_array_column,
       coalesce(remarks, '') as comment
from svv_all_columns
where database_name = $1
  and schema_name not in ('pg_catalog', 'information_schema', 'pg_automv')
order by database, schema, "table", position