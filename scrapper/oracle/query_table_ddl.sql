WITH FUNCTION synq_table_ddl(p_owner IN VARCHAR2, p_table IN VARCHAR2) RETURN CLOB IS
BEGIN
    -- Without storage and segment attributes (tablespace, PCTFREE, extents,
    -- logging, compression), so the text changes with the table's shape only.
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
    RETURN DBMS_METADATA.GET_DDL('TABLE', p_table, p_owner);
END;
SELECT synq_table_ddl(:1, :2) AS "sql" FROM DUAL
