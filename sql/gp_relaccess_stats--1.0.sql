/* gp_relaccess_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relaccess_stats" to load this file. \quit

CREATE TABLE relaccess_stats (
    dbid Oid,
    relid Oid,
    userid Oid,
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
