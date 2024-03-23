/* gp_relaccess_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relaccess_stats" to load this file. \quit

CREATE TABLE relaccess_stats (
    dbid Oid,
    relid Oid,
    relname Name,
    last_user_id Oid,
    last_read timestamp,
    last_write timestamp,
    n_select_queries int,
    n_insert_queries int,
    n_update_queries int,
    n_delete_queries int,
    n_truncate_queries int
) DISTRIBUTED BY (dbid, relid);

CREATE FUNCTION relaccess_stats_update()
RETURNS void
AS 'MODULE_PATHNAME', 'relaccess_stats_update'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION __relaccess_upsert_from_dump_file(path varchar) RETURNS VOID
LANGUAGE plpgsql VOLATILE EXECUTE ON MASTER AS
$func$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_update_tmp';

    EXECUTE 'CREATE TEMP TABLE relaccess_stats_update_tmp (LIKE relaccess_stats) distributed by (dbid, relid)';

    EXECUTE format('COPY relaccess_stats_update_tmp FROM ''%s'' WITH (FORMAT ''csv'', DELIMITER '','')', $1);

    EXECUTE 'INSERT INTO relaccess_stats
        SELECT dbid, relid, relname, last_user_id, last_read, last_write, 0, 0, 0, 0, 0 FROM relaccess_stats_update_tmp stage
        WHERE NOT EXISTS (
            SELECT 1 FROM relaccess_stats orig WHERE orig.dbid = stage.dbid AND orig.relid = stage.relid)';

    EXECUTE 'UPDATE relaccess_stats orig SET
        relname = stage.relname,
        last_user_id = stage.last_user_id,
        last_read = stage.last_read,
        last_write = stage.last_write,
        n_select_queries = orig.n_select_queries + stage.n_select_queries,
        n_insert_queries = orig.n_insert_queries + stage.n_insert_queries,
        n_update_queries = orig.n_update_queries + stage.n_update_queries,
        n_delete_queries = orig.n_delete_queries + stage.n_delete_queries,
        n_truncate_queries = orig.n_truncate_queries + stage.n_truncate_queries
    FROM relaccess_stats_update_tmp stage
        WHERE orig.dbid = stage.dbid AND orig.relid = stage.relid';

    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_update_tmp';
END
$func$;
