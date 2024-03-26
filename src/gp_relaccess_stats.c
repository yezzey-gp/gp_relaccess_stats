#include "postgres.h"
#include "access/xact.h"
#include "access/hash.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_database.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pg_config_ext.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"

#include <stdlib.h>

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(relaccess_stats_update);

static void relaccess_stats_update_internal(void);
static void relaccess_dump_to_files(bool only_this_db);
static void relaccess_dump_to_files_internal(HTAB *files);
static void relaccess_upsert_from_file(void);
static void recover_leftover_dump(void);
static void relaccess_shmem_startup(void);
static void relaccess_shmem_shutdown(int code, Datum arg);
static uint32 relaccess_hash_fn(const void *key, Size keysize);
static int relaccess_match_fn(const void *key1, const void *key2, Size keysize);
static uint32 local_relaccess_hash_fn(const void *key, Size keysize);
static int local_relaccess_match_fn(const void *key1, const void *key2,
                                    Size keysize);
static bool collect_relaccess_hook(List *rangeTable, bool ereport_on_violation);
static void relaccess_xact_callback(XactEvent event, void *arg);
static void collect_truncate_hook(Node *parsetree, const char *queryString,
                                  ProcessUtilityContext context,
                                  ParamListInfo params, DestReceiver *dest,
                                  char *completionTag);
static void relaccess_executor_end_hook(QueryDesc *query_desc);
static void relaccess_drop_hook(ObjectAccessType access, Oid classId,
                                Oid objectId, int subId, void *arg);
static void memorize_local_access_entry(Oid relid, AclMode perms);
static void update_relname_cache(Oid relid, char *relname);
static StringInfoData get_dump_filename(Oid dbid);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorCheckPerms_hook_type prev_check_perms_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static object_access_hook_type prev_object_access_hook = NULL;

typedef struct relaccessHashKey {
  Oid dbid;
  Oid relid;
} relaccessHashKey;

typedef struct relaccessEntry {
  relaccessHashKey key;
  NameData relname;
  Oid userid;
  TimestampTz last_read;
  TimestampTz last_write;
  int64 n_select;
  int64 n_insert;
  int64 n_update;
  int64 n_delete;
  int64 n_truncate;
} relaccessEntry;

typedef struct relaccessGlobalData {
  LWLock *relaccess_ht_lock;
  LWLock *relaccess_file_lock;
} relaccessGlobalData;

typedef struct localAccessKey {
  Oid relid;
  int stmt_cnt;
} localAccessKey;

typedef struct localAccessEntry {
  localAccessKey key;
  Timestamp when;
  AclMode perms;
} localAccessEntry;

typedef struct relnameCacheEntry {
  Oid relid;
  char *relname;
} relnameCacheEntry;

typedef struct fileDumpEntry {
  Oid dbid;
  char *filename;
  FILE *file;
} fileDumpEntry;

static int32 relaccess_size;
static bool dump_on_overflow;
static bool is_enabled;
static relaccessGlobalData *data;
static HTAB *relaccesses;
static HTAB *local_access_entries = NULL;
static const int32 LOCAL_HTAB_SZ = 128;
static HTAB *relname_cache = NULL;
static const int32 RELCACHE_SZ = 16;
static const int32 FILE_CACHE_SZ = 16;
static int stmt_counter = 0;
static bool had_ht_overflow = false;

#define IS_POSTGRES_DB                                                         \
  (strcmp("postgres", get_database_name(MyDatabaseId)) == 0)

static void relaccess_shmem_startup() {
  bool found;
  HASHCTL info;

  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  data = (relaccessGlobalData *)(ShmemInitStruct(
      "relaccess_stats", sizeof(relaccessGlobalData), &found));
  if (!found) {
    data->relaccess_ht_lock = LWLockAssign();
    data->relaccess_file_lock = LWLockAssign();
  }

  memset(&info, 0, sizeof(info));
  info.keysize = sizeof(relaccessHashKey);
  info.entrysize = sizeof(relaccessEntry);
  info.hash = relaccess_hash_fn;
  info.match = relaccess_match_fn;
  relaccesses =
      ShmemInitHash("relaccess_stats hash", relaccess_size, relaccess_size,
                    &info, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

  LWLockRelease(AddinShmemInitLock);

  if (!IsUnderPostmaster) {
    on_shmem_exit(relaccess_shmem_shutdown, (Datum)0);
  }
}

static void relaccess_shmem_shutdown(int code, Datum arg) {
  if (code || !data || !relaccesses) {
    return;
  }
  LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
  LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
  relaccess_dump_to_files(false);
  LWLockRelease(data->relaccess_ht_lock);
  LWLockRelease(data->relaccess_file_lock);
}

static uint32 relaccess_hash_fn(const void *key, Size keysize) {
  const relaccessHashKey *k = (const relaccessHashKey *)key;
  return hash_uint32((uint32)k->dbid) ^ hash_uint32((uint32)k->relid);
}

static int relaccess_match_fn(const void *key1, const void *key2,
                              Size keysize) {
  const relaccessHashKey *k1 = (const relaccessHashKey *)key1;
  const relaccessHashKey *k2 = (const relaccessHashKey *)key2;
  return (k1->dbid == k2->dbid && k1->relid == k2->relid ? 0 : 1);
}

static uint32 local_relaccess_hash_fn(const void *key, Size keysize) {
  const localAccessKey *k = (const localAccessKey *)key;
  return hash_uint32((uint32)k->stmt_cnt) ^ hash_uint32((uint32)k->relid);
}

static int local_relaccess_match_fn(const void *key1, const void *key2,
                                    Size keysize) {
  const localAccessKey *k1 = (const localAccessKey *)key1;
  const localAccessKey *k2 = (const localAccessKey *)key2;
  return (k1->stmt_cnt == k2->stmt_cnt && k1->relid == k2->relid ? 0 : 1);
}

void _PG_init(void) {
  Size size;
  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }
  if (!process_shared_preload_libraries_in_progress) {
    return;
  }

  DefineCustomIntVariable(
      "gp_relaccess_stats.max_tables",
      "Sets the maximum number of tables cached by gp_relaccess_stats.", NULL,
      &relaccess_size, 65536, 128, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL,
      NULL);

  DefineCustomBoolVariable("gp_relaccess_stats.dump_on_overflow",
                           "Selects whether we should dump to .csv in case "
                           "gp_relaccess_stats.max_tables is exceeded.",
                           NULL, &dump_on_overflow, false, PGC_SIGHUP, 0, NULL,
                           NULL, NULL);

  DefineCustomBoolVariable(
      "gp_relaccess_stats.enabled",
      "Collect table access stats globally or for a specific database. "
      "Note that shared memory is initialized indepemdent of this argument.",
      NULL, &is_enabled, false, PGC_SUSET, 0, NULL, NULL, NULL);

  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = relaccess_shmem_startup;
  prev_check_perms_hook = ExecutorCheckPerms_hook;
  ExecutorCheckPerms_hook = collect_relaccess_hook;
  next_ProcessUtility_hook = ProcessUtility_hook;
  ProcessUtility_hook = collect_truncate_hook;
  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = relaccess_executor_end_hook;
  prev_object_access_hook = object_access_hook;
  object_access_hook = relaccess_drop_hook;
  RequestAddinLWLocks(2);
  size = MAXALIGN(sizeof(relaccessGlobalData));
  size = add_size(size,
                  hash_estimate_size(relaccess_size, sizeof(relaccessEntry)));
  RequestAddinShmemSpace(size);
  RegisterXactCallback(relaccess_xact_callback, NULL);
  HASHCTL ctl;
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(localAccessKey);
  ctl.entrysize = sizeof(localAccessEntry);
  ctl.hash = local_relaccess_hash_fn;
  ctl.match = local_relaccess_match_fn;
  local_access_entries =
      hash_create("Transaction-wide relaccess entries", LOCAL_HTAB_SZ, &ctl,
                  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(Oid);
  ctl.entrysize = sizeof(relnameCacheEntry);
  ctl.hash = oid_hash;
  relname_cache = hash_create("Transaction-wide relation name cache",
                              RELCACHE_SZ, &ctl, HASH_ELEM | HASH_FUNCTION);
}

void _PG_fini(void) {
  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }
  shmem_startup_hook = prev_shmem_startup_hook;
  ExecutorCheckPerms_hook = prev_check_perms_hook;
  ProcessUtility_hook = next_ProcessUtility_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
  object_access_hook = prev_object_access_hook;
}

static bool collect_relaccess_hook(List *rangeTable,
                                   bool ereport_on_violation) {
  if (prev_check_perms_hook &&
      !prev_check_perms_hook(rangeTable, ereport_on_violation)) {
    return false;
  }
  if (Gp_role == GP_ROLE_DISPATCH && is_enabled) {
    ListCell *l;
    foreach (l, rangeTable) {
      RangeTblEntry *rte = (RangeTblEntry *)lfirst(l);
      if (rte->rtekind != RTE_RELATION) {
        continue;
      }
      Oid relid = rte->relid;
      AclMode requiredPerms = rte->requiredPerms;
      if (requiredPerms &
          (ACL_SELECT | ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE)) {
        memorize_local_access_entry(relid, requiredPerms);
        update_relname_cache(relid, NULL);
      }
    }
  }
  return true;
}

static void collect_truncate_hook(Node *parsetree, const char *queryString,
                                  ProcessUtilityContext context,
                                  ParamListInfo params, DestReceiver *dest,
                                  char *completionTag) {
  if (nodeTag(parsetree) == T_TruncateStmt && is_enabled &&
      Gp_role == GP_ROLE_DISPATCH) {
    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);
    TruncateStmt *stmt = (TruncateStmt *)parsetree;
    ListCell *cell;
    /**
     *  TODO: TRUNCATE may be called with ONLY option which limits it only to
     *the root partition. Otherwise it will truncate all child partitions. We
     *might wish to track the difference by explicitly adding records for each
     *truncated partition in the future
     **/
    foreach (cell, stmt->relations) {
      RangeVar *rv = lfirst(cell);
      Relation rel;
      rel = heap_openrv(rv, AccessExclusiveLock);
      memorize_local_access_entry(rel->rd_id, ACL_TRUNCATE);
      update_relname_cache(rel->rd_id, pstrdup(rv->relname));
      heap_close(rel, NoLock);
    }
    MemoryContextSwitchTo(oldcontext);
  }
  if (next_ProcessUtility_hook) {
    next_ProcessUtility_hook(parsetree, queryString, context, params, dest,
                             completionTag);
  } else {
    standard_ProcessUtility(parsetree, queryString, context, params, dest,
                            completionTag);
  }
}

#define UPDATE_STAT(lowercase, uppercase)                                      \
  dst_entry->n_##lowercase += (src_entry->perms & ACL_##uppercase ? 1 : 0)

// if there is a better way to cleanup a postgres hashtable
// w/o recreating it, I didn't find it
#define CLEAR_HTAB(entryType, hmap, key_name)                                  \
  {                                                                            \
    HASH_SEQ_STATUS hash_seq;                                                  \
    entryType *src_entry;                                                      \
    hash_seq_init(&hash_seq, hmap);                                            \
    while ((src_entry = hash_seq_search(&hash_seq)) != NULL) {                 \
      bool found;                                                              \
      hash_search(hmap, &src_entry->key_name, HASH_REMOVE, &found);            \
      Assert(found);                                                           \
    }                                                                          \
  }

static void relaccess_xact_callback(XactEvent event, void * /*arg*/) {
  if (Gp_role != GP_ROLE_DISPATCH || !is_enabled) {
    return;
  }
  Assert(GetCurrentTransactionNestLevel == 1);
  if (event == XACT_EVENT_COMMIT) {
    HASH_SEQ_STATUS hash_seq;
    localAccessEntry *src_entry;
    hash_seq_init(&hash_seq, local_access_entries);
    while ((src_entry = hash_seq_search(&hash_seq)) != NULL) {
      LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
      bool found;
      relaccessHashKey key;
      key.dbid = MyDatabaseId;
      key.relid = src_entry->key.relid;
      long n_access_records = hash_get_num_entries(relaccesses);
      relaccessEntry *dst_entry = NULL;
      Assert(n_access_records <= relaccess_size);
      if (n_access_records == relaccess_size) {
        dst_entry =
            (relaccessEntry *)hash_search(relaccesses, &key, HASH_FIND, &found);
      } else {
        dst_entry = (relaccessEntry *)hash_search(relaccesses, &key,
                                                  HASH_ENTER_NULL, &found);
      }
      if (dst_entry || dump_on_overflow) {
        if (!dst_entry) {
          // TODO: figure out the right locking scheme. For now we're safe here,
          // as whenever we hold relaccess_ht_lock we also hold the file lock.
          // But it might change. Hence, this line needs to be fixed
          relaccess_dump_to_files(false);
          // we MUST have enough space now
          dst_entry = (relaccessEntry *)hash_search(relaccesses, &key,
                                                    HASH_ENTER_NULL, &found);
          Assert(dst_entry != NULL);
        }
        if (!found) {
          dst_entry->key = key;
          dst_entry->userid = GetUserId();
          dst_entry->last_read = 0;
          dst_entry->last_write = 0;
          dst_entry->n_select = 0;
          dst_entry->n_insert = 0;
          dst_entry->n_update = 0;
          dst_entry->n_delete = 0;
          dst_entry->n_truncate = 0;
        }
        UPDATE_STAT(select, SELECT);
        UPDATE_STAT(insert, INSERT);
        UPDATE_STAT(update, UPDATE);
        UPDATE_STAT(delete, DELETE);
        UPDATE_STAT(truncate, TRUNCATE);
        if (src_entry->perms & (ACL_SELECT)) {
          dst_entry->last_read =
              Max(GetCurrentTimestamp(), dst_entry->last_read);
        }
        if (src_entry->perms &
            (ACL_INSERT | ACL_DELETE | ACL_UPDATE | ACL_TRUNCATE)) {
          dst_entry->last_write =
              Max(GetCurrentTimestamp(), dst_entry->last_write);
        }
        relnameCacheEntry *namecache_entry = (relnameCacheEntry *)hash_search(
            relname_cache, &key.relid, HASH_ENTER, &found);
        Assert(namecache_entry);
        strcpy(dst_entry->relname.data, namecache_entry->relname);
        int namelen = strlen(namecache_entry->relname);
        dst_entry->relname.data[namelen] = 0;
      } else {
        if (!had_ht_overflow) {
          elog(WARNING, "gp_relaccess_stats.max_tables is exceeded! New table "
                        "events will be lost. "
                        "Please execute relaccess_stats_update() and consider "
                        "setting a hihger value");
        }
        had_ht_overflow = true;
      }
      LWLockRelease(data->relaccess_ht_lock);
      hash_search(local_access_entries, &src_entry->key, HASH_REMOVE, &found);
      Assert(found);
    }
  } else if (event == XACT_EVENT_ABORT) {
    CLEAR_HTAB(localAccessEntry, local_access_entries, key);
  }
  CLEAR_HTAB(relnameCacheEntry, relname_cache, relid);
}

Datum relaccess_stats_update(PG_FUNCTION_ARGS) {
  relaccess_stats_update_internal();
  PG_RETURN_VOID();
}

static void relaccess_stats_update_internal() {
  LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
  recover_leftover_dump();
  LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
  relaccess_dump_to_files(true);
  LWLockRelease(data->relaccess_ht_lock);
  relaccess_upsert_from_file();
  StringInfoData filename = get_dump_filename(MyDatabaseId);
  unlink(filename.data);
  pfree(filename.data);
  LWLockRelease(data->relaccess_file_lock);
}

static void add_file_dump_entry(Oid dbid, HTAB *ht) {
  bool found;
  fileDumpEntry *file_entry = hash_search(ht, &dbid, HASH_ENTER, &found);
  if (!found) {
    file_entry->dbid = dbid;
    StringInfoData filename = get_dump_filename(file_entry->dbid);
    file_entry->filename = filename.data;
    file_entry->file = AllocateFile(file_entry->filename, "at");
  }
}

static void relaccess_dump_to_files(bool only_this_db) {
  HTAB *file_mapping;
  HASHCTL ctl;
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(Oid);
  ctl.entrysize = sizeof(fileDumpEntry);
  ctl.hash = oid_hash;
  file_mapping = hash_create("Relaccess dump files", FILE_CACHE_SZ, &ctl,
                             HASH_ELEM | HASH_FUNCTION);
  if (only_this_db) {
    add_file_dump_entry(MyDatabaseId, file_mapping);
  } else {
    HASH_SEQ_STATUS hash_seq;
    relaccessEntry *access_entry;
    hash_seq_init(&hash_seq, relaccesses);
    while ((access_entry = hash_seq_search(&hash_seq)) != NULL) {
      add_file_dump_entry(access_entry->key.dbid, file_mapping);
    }
  }
  relaccess_dump_to_files_internal(file_mapping);
  HASH_SEQ_STATUS hash_seq;
  hash_seq_init(&hash_seq, file_mapping);
  fileDumpEntry *entry;
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    FreeFile(entry->file);
    pfree(entry->filename);
  }
  hash_destroy(file_mapping);
}

static void relaccess_dump_to_files_internal(HTAB *files) {
  // Dump to tmp .csv file and clear the HT
  HASH_SEQ_STATUS hash_seq;
  relaccessEntry *entry;
  StringInfoData entry_csv_line;
  initStringInfo(&entry_csv_line);
  hash_seq_init(&hash_seq, relaccesses);
  // alas, timestamptz_to_str isn't safe to be called twice in appendStringInfo
  char read_time_buf[MAXDATELEN + 1];
  char write_time_buf[MAXDATELEN + 1];
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    bool found;
    fileDumpEntry *dumpfile =
        hash_search(files, &entry->key.dbid, HASH_FIND, &found);
    if (!found) {
      // ignore this dbid
      continue;
    }
    strncpy(read_time_buf, timestamptz_to_str(entry->last_read), MAXDATELEN);
    strncpy(write_time_buf, timestamptz_to_str(entry->last_write), MAXDATELEN);
    read_time_buf[MAXDATELEN] = 0;
    write_time_buf[MAXDATELEN] = 0;
    appendStringInfo(
        &entry_csv_line, "%d,\"%s\",%d,\"%s\",\"%s\",%ld,%ld,%ld,%ld,%ld\n",
        entry->key.relid, entry->relname.data, entry->userid, read_time_buf,
        write_time_buf, entry->n_select, entry->n_insert, entry->n_update,
        entry->n_delete, entry->n_truncate);
    if (fwrite(entry_csv_line.data, 1, entry_csv_line.len, dumpfile->file) !=
        entry_csv_line.len) {
      hash_seq_term(&hash_seq);
      // TODO: handle
      break;
    }
    resetStringInfo(&entry_csv_line);
    // TODO: figure out a safer way to remove entries from HT.
    // If for some reason we fail upserting dumped data somewhere later
    // those stats are forever lost to us
    hash_search(relaccesses, &entry->key, HASH_REMOVE, &found);
    had_ht_overflow = false;
    Assert(found);
  }
}

static void relaccess_upsert_from_file() {
  SPI_connect();
  StringInfoData filename = get_dump_filename(MyDatabaseId);
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query, "SELECT __relaccess_upsert_from_dump_file('%s')",
                   filename.data);
  SPI_execute(query.data, false, 1);
  SPI_finish();
  pfree(filename.data);
  pfree(query.data);
}

static void recover_leftover_dump() {
  StringInfoData filename = get_dump_filename(MyDatabaseId);
  FILE *f = AllocateFile(filename.data, "rt");
  if (f == NULL) {
    pfree(filename.data);
    return;
  }
  relaccess_upsert_from_file();
  FreeFile(f);
  unlink(filename.data);
  pfree(filename.data);
}

static void update_relname_cache(Oid relid, char *relname) {
  bool found;
  relnameCacheEntry *relname_entry = (relnameCacheEntry *)hash_search(
      relname_cache, &relid, HASH_ENTER, &found);
  if (!found) {
    relname_entry->relid = relid;
    if (!relname) {
      MemoryContext oldcontext;
      oldcontext = MemoryContextSwitchTo(TopTransactionContext);
      relname_entry->relname = get_rel_name(relid);
      MemoryContextSwitchTo(oldcontext);
    } else {
      relname_entry->relname = relname;
    }
  } else {
    /**
     * NOTE: as we don't handle the 'else' clause here, there will be cases when
     * we write outdated table names, like below:
     *    BEGIN;
     *      INSERT INTO tbl VALUES (1);
     *      ALTER TABLE tbl RENAME TO new_tbl;
     *      SELECT * FROM new_tbl;
     *    COMMIT;
     * In this case both INSERT and SELECT stmts would be counted with the
     * old'tbl' name, as we don't update our cache for already known relids in
     * the same transaction. This is a deliberate decision for performance
     * reasons.
     */
  }
}

static void memorize_local_access_entry(Oid relid, AclMode perms) {
  bool found;
  localAccessKey key;
  key.stmt_cnt = stmt_counter;
  key.relid = relid;
  localAccessEntry *entry = (localAccessEntry *)hash_search(
      local_access_entries, &key, HASH_ENTER, &found);
  if (!found) {
    entry->key = key;
    entry->when = GetCurrentTimestamp();
    entry->perms = perms;
  } else {
    entry->perms |= perms;
  }
}

static void relaccess_executor_end_hook(QueryDesc *query_desc) {
  if (prev_ExecutorEnd_hook) {
    prev_ExecutorEnd_hook(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
  // Unfortunately, we cannot safely rely on gp_command_counter as
  // it is being incremented more than once for many statements.
  // So we have to maintain our own statement counter.
  stmt_counter++;
}

static StringInfoData get_dump_filename(Oid dbid) {
  StringInfoData filename;
  initStringInfoOfSize(&filename, 256);
  appendStringInfo(&filename, "%s/relaccess_stats_dump_%d.csv",
                   PGSTAT_STAT_PERMANENT_DIRECTORY, dbid);
  return filename;
}

static void relaccess_drop_hook(ObjectAccessType access, Oid classId,
                                Oid objectId, int subId, void *arg) {
  if (prev_object_access_hook) {
    prev_object_access_hook(access, classId, objectId, subId, arg);
  }
  if (classId == DatabaseRelationId && access == OAT_DROP) {
    LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
    HASH_SEQ_STATUS hash_seq;
    relaccessEntry *entry;
    hash_seq_init(&hash_seq, relaccesses);
    while ((entry = hash_seq_search(&hash_seq)) != NULL) {
      if (entry->key.dbid == objectId) {
        bool found;
        hash_search(relaccesses, &entry->key, HASH_REMOVE, &found);
        had_ht_overflow = false;
      }
    }
    LWLockRelease(data->relaccess_ht_lock);
    LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
    StringInfoData filename = get_dump_filename(objectId);
    unlink(filename.data);
    pfree(filename.data);
    LWLockRelease(data->relaccess_file_lock);
  }
}