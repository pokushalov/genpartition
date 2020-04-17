sql = {
    "all_indexes":
        {
            "sql": "select * from dba_indexes where owner = '{}' and table_name = '{}'",
            "desc": "sql for get all indexes for current table, [owner, table_name]"
        },
    "all_tables":
        {
            "sql": "select * from dba_tables where owner = '{}' and table_name = '{}'",
            "desc": "sql for get all tables for current table, [owner, table_name]"
        },
    "object_ddl":
        {
            "sql": "select dbms_metadata.get_ddl(object_type => upper('{}'), schema => upper('{}'), name => upper('{}') ) from dual",
            "desc": "sql for get DDL tables for current table, [object_name, object_owner]"
        },
    "all_constraints":
        {
            "sql": "select * from dba_constraints where owner = '{}' and table_name = '{}' ",
            "desc": "sql for get all constraitns for  current table, [object_name, object_owner]"
        },
    "all_triggers":
        {
            "sql": "select owner, trigger_name from dba_triggers where owner = '{}' and table_name = '{}' ",
            "desc": "sql for get all triggers  for  current table, [object_name, object_owner]"
        },
    "table_size":
        {
            "sql": "select  sum(bytes) from dba_segments where  owner = '{}' and segment_name = '{}' group by segment_name, owner",
            "desc": "sql for get space needed for  current table, [object_name, object_owner]"
        },
    "tablespace_free_space":
        {
            "sql": "select tablespace_name, round(sum(bytes)) FreeSpace from dba_free_space where tablespace_name = '{}'  group by tablespace_name ",
            "desc": "sql to get free space in tablespace [object_name, object_owner]"
        },
    "prepare_extract_ddl":
        {
            "sql": """
                        begin 
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', TRUE);
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
                            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);
                        end;
                    """,
            "desc": "Prepare session for extracting DDL"
        },
    "get_all_grants":
        {
            "sql": "select 'grant ' || privilege || ' on ' || owner || '.' || table_name || ' to ' || grantee || ';'  from dba_tab_privs where owner='{}' and table_name = '{}'",
            "desc": "Getting all grants for object"
        }
}
