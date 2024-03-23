#include "postgres.h"
#include "access/xact.h"
#include "access/hash.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pg_config_ext.h"
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
static void relaccess_dump_to_file(void);
static void relaccess_upsert_from_file(void);
static void recover_leftover_dump(void);
static void relaccess_shmem_startup(void);
static void relaccess_shmem_shutdown(int code, Datum arg);
static uint32 relaccess_hash_fn(const void *key, Size keysize);
static int relaccess_match_fn(const void *key1, const void *key2, Size keysize);
static uint32 local_relaccess_hash_fn(const void *key, Size keysize);
static int local_relaccess_match_fn(const void *key1, const void *key2,
                                    Size keysize);
static uint32 relnamecache_hash_fn(const void *key, Size keysize);
static int relnamecache_match_fn(const void *key1, const void *key2,
                                 Size keysize);
static bool collect_relaccess_hook(List *rangeTable, bool ereport_on_violation);
static void relaccess_xact_callback(XactEvent event, void *arg);
static void collect_truncate_hook(Node *parsetree, const char *queryString,
                                  ProcessUtilityContext context,
                                  ParamListInfo params, DestReceiver *dest,
                                  char *completionTag);
static void relaccess_executor_end_hook(QueryDesc *query_desc);
static void memorize_local_access_entry(Oid relid, AclMode perms);
static void update_relname_cache(Oid relid, char *relname);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorCheckPerms_hook_type prev_check_perms_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

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

static relaccessGlobalData *data;
static HTAB *relaccesses;
static const int32 RELACCESS_MAX = 100000;
static HTAB *local_access_entries = NULL;
static const int32 LOCAL_HTAB_SZ = 128;
static HTAB *relname_cache = NULL;
static const int32 RELCACHE_SZ = 16;
static int stmt_counter = 0;

static const char *const DUMP_FILENAME = "relaccess_stats_dump.csv";

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
      ShmemInitHash("relaccess_stats hash", RELACCESS_MAX, RELACCESS_MAX, &info,
                    HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

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
  relaccess_dump_to_file();
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

static uint32 relnamecache_hash_fn(const void *key, Size keysize) {
  const Oid *k = (const Oid *)key;
  return hash_uint32((uint32)(*k));
}

static int relnamecache_match_fn(const void *key1, const void *key2,
                                 Size keysize) {
  const Oid *k1 = (const Oid *)key1;
  const Oid *k2 = (const Oid *)key2;
  return (*k1 == *k2 ? 0 : 1);
}

void _PG_init(void) {
  Size size;
  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }
  if (!process_shared_preload_libraries_in_progress) {
    return;
  }
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = relaccess_shmem_startup;
  prev_check_perms_hook = ExecutorCheckPerms_hook;
  ExecutorCheckPerms_hook = collect_relaccess_hook;
  next_ProcessUtility_hook = ProcessUtility_hook;
  ProcessUtility_hook = collect_truncate_hook;
  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = relaccess_executor_end_hook;
  RequestAddinLWLocks(2);
  size = MAXALIGN(sizeof(relaccessGlobalData));
  size =
      add_size(size, hash_estimate_size(RELACCESS_MAX, sizeof(relaccessEntry)));
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
  ctl.hash = relnamecache_hash_fn;
  ctl.match = relnamecache_match_fn;
  relname_cache =
      hash_create("Transaction-wide relation name cache", RELCACHE_SZ, &ctl,
                  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

void _PG_fini(void) {
  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }
  shmem_startup_hook = prev_shmem_startup_hook;
  ExecutorCheckPerms_hook = prev_check_perms_hook;
  ProcessUtility_hook = next_ProcessUtility_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}

static bool collect_relaccess_hook(List *rangeTable,
                                   bool ereport_on_violation) {
  if (prev_check_perms_hook &&
      !prev_check_perms_hook(rangeTable, ereport_on_violation)) {
    return false;
  }
  if (Gp_role != GP_ROLE_DISPATCH) {
    return true;
  }
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
  return true;
}

static void collect_truncate_hook(Node *parsetree, const char *queryString,
                                  ProcessUtilityContext context,
                                  ParamListInfo params, DestReceiver *dest,
                                  char *completionTag) {
  if (nodeTag(parsetree) == T_TruncateStmt) {
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
  if (Gp_role != GP_ROLE_DISPATCH) {
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
      relaccessEntry *dst_entry =
          (relaccessEntry *)hash_search(relaccesses, &key, HASH_ENTER, &found);
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
        dst_entry->last_read = Max(GetCurrentTimestamp(), dst_entry->last_read);
      }
      if (src_entry->perms &
          (ACL_INSERT | ACL_DELETE | ACL_UPDATE | ACL_TRUNCATE)) {
        dst_entry->last_write =
            Max(GetCurrentTimestamp(), dst_entry->last_write);
      }
      relnameCacheEntry *relcache_entry = (relnameCacheEntry *)hash_search(
          relname_cache, &key.relid, HASH_ENTER_NULL, &found);
      Assert(relcache_entry);
      strcpy(dst_entry->relname.data, relcache_entry->relname);
      int namelen = strlen(relcache_entry->relname);
      dst_entry->relname.data[namelen] = 0;
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
  if (!IS_POSTGRES_DB) {
    elog(WARNING, "relaccess_stats_update should only ever be called from "
                  "'postgres' database");
    return;
  }
  LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
  recover_leftover_dump();
  relaccess_dump_to_file();
  relaccess_upsert_from_file();
  unlink(DUMP_FILENAME);
  LWLockRelease(data->relaccess_file_lock);
}

static void relaccess_dump_to_file() {
  unlink(DUMP_FILENAME);
  FILE *file = AllocateFile(DUMP_FILENAME, "wt");
  if (file == NULL) {
    // TODO: handle
  }
  LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
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
    strncpy(read_time_buf, timestamptz_to_str(entry->last_read), MAXDATELEN);
    strncpy(write_time_buf, timestamptz_to_str(entry->last_write), MAXDATELEN);
    read_time_buf[MAXDATELEN] = 0;
    write_time_buf[MAXDATELEN] = 0;
    appendStringInfo(
        &entry_csv_line, "%d,%d,\"%s\",%d,\"%s\",\"%s\",%ld,%ld,%ld,%ld,%ld\n",
        entry->key.dbid, entry->key.relid, entry->relname.data, entry->userid,
        read_time_buf, write_time_buf, entry->n_select, entry->n_insert,
        entry->n_update, entry->n_delete, entry->n_truncate);
    if (fwrite(entry_csv_line.data, 1, entry_csv_line.len, file) !=
        entry_csv_line.len) {
      hash_seq_term(&hash_seq);
      // TODO: handle
      break;
    }
    resetStringInfo(&entry_csv_line);
    // TODO: figure out a safer way to remove entries from HT.
    // If for some reason we fail upserting dumped data somewhere later
    // those stats are forever lost for us
    bool found;
    hash_search(relaccesses, &entry->key, HASH_REMOVE, &found);
    Assert(found);
  }
  LWLockRelease(data->relaccess_ht_lock);
  FreeFile(file);
}

static void relaccess_upsert_from_file() {
  SPI_connect();
  SPI_execute(
      "SELECT __relaccess_upsert_from_dump_file('relaccess_stats_dump.csv')",
      false, 1);
  SPI_finish();
}

static void recover_leftover_dump() {
  FILE *f = AllocateFile(DUMP_FILENAME, "rt");
  if (f == NULL) {
    return;
  }
  relaccess_upsert_from_file();
  FreeFile(f);
  unlink(DUMP_FILENAME);
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