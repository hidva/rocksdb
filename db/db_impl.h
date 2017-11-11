// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "include/db.h"
#include "include/env.h"
#include "port/port.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

// 建议看 db 主要操作时如: 打开, 写等, 列出主要的执行流程, 并考虑各种异常情况下的可恢复性.
class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, uint64_t* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [begin,end]
  void TEST_CompactRange(
      int level,
      const std::string& begin,
      const std::string& end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

 private:
  friend class DB;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

  // 会创建并初始化 dbname 指定的数据库, dbname 原数据会全部丢失.
  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  // recover recently logged updates 所产生的 Any changes to be made to the descriptor are added to
  // *edit.
  Status Recover(VersionEdit* edit);

  // Apply the specified updates and save the resulting descriptor to
  // persistent storage.  If cleanup_mem is non-NULL, arrange to
  // delete it when all existing snapshots have gone away iff VersionSet::LogAndApply()
  // returns OK.
  // Q: delete it when all existing snapshots have gone away. 啥意思?
  /* 此时 edit 表示一堆尚未持久化以及尚未应用到 VersionSet 中的变更. Install() 将 edit 持久化以及引用到
   * versionset 中. 可以参考实现了解 new_log_number, cleanup_mem 的语义.
   *
   * 另外一个角度来看, edit 存储着 redo old log 所产生的 server state 变更记录, new log number 指定了已经打开
   * 的 new log 的 number, Install() 会原子地完成如下事情:
   *    1. 持久化与 apply edit -> version set;
   *    2. 持久化与 apply new log number;
   * 既当 Install() 成功返回时, old log 可以被安全地删除. 当 Install() 出错时, old log 需要保留, 并且总是
   * 可以再一次 redo old log.
   */
  Status Install(VersionEdit* edit,
                 uint64_t new_log_number,
                 MemTable* cleanup_mem);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  // 这里 stale in-memory entries 是指 table cache 中已经被持久化设备删除但是仍然存在于 table cache 中的项.
  void DeleteObsoleteFiles();

  // Called when an iterator over a particular version of the
  // descriptor goes away.
  static void Unref(void* arg1, void* arg2);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // 严格地说, 这里并没有 write a new descriptor; 而是更新一个已经存在的 descriptor.
  // CompactMemTable() 具体的操作流程可以参见实现.
  Status CompactMemTable();

  // log file 中的 record 是 WriteBatch 序列化后的内容.
  // max_sequence 是指 recover log file 的过程中遇到的最大的 sequence number.
  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence);

  // 将 mem 写入 disk 中, 同时将对 server state 的更新记录在 edit 中.
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit);

  bool HasLargeValues(const WriteBatch& batch) const;

  // Process data in "*updates" and return a status.  "assigned_seq"
  // is the sequence number assigned to the first mod in "*updates".
  // If no large values are encountered, "*final" is set to "updates".
  // If large values were encountered, registers the references of the
  // large values with the VersionSet, writes the large values to
  // files (if appropriate), and allocates a new WriteBatch with the
  // large values replaced with indirect references and stores a
  // pointer to the new WriteBatch in *final.  If *final != updates on
  // return, then the client should delete *final when no longer
  // needed.  Returns OK on success, and an appropriate error
  // otherwise.
  Status HandleLargeValues(SequenceNumber assigned_seq,
                           WriteBatch* updates,
                           WriteBatch** final);

  // Helper routine for HandleLargeValues
  void MaybeCompressLargeValue(
      const Slice& raw_value,
      Slice* file_bytes,
      std::string* scratch,
      LargeValueRef* ref);

  struct CompactionState;

  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  // 执行 compact 操作, background 表明 compact 是后台运行着的.
  void BackgroundCompaction();
  void CleanupCompaction(CompactionState* compact);
  Status DoCompactionWork(CompactionState* compact);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;         // Signalled when !bg_compaction_scheduled_
  port::CondVar compacting_cv_;  // Signalled when !compacting_
  // 本来我觉得这里的 last_sequence_ 是用来作为 sst table file name 的数字后缀呢, 后来发现不太可能是==
  SequenceNumber last_sequence_;
  MemTable* mem_;
  // log_ 与 mem_ 对应, 任何一个 write batch 在应用到 mem 之前都会写入 log 中.
  WritableFile* logfile_;
  log::Writer* log_;
  // QA: log_number 干什么吃的?
  // A: log file 对应的 log number.
  uint64_t log_number_;
  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions. 其中存放的是 file number, 我本来以为是 file fd 呢==
  std::set<uint64_t> pending_outputs_;

  /*
   * 按我理解 leveldb 在 schedule bg compaction 之后将 bg_compaction_scheduled_ 置为 True.
   * bg compaction 的实现可能如下:
   *    func bg_compaction() {
   *        mutex_.lock()
   *        compacting_ = True;
   *        mutex_.unlock()
   *        do_compaction(); // 这个期间可能不会持有着锁.
   *        mutex_.lock()
   *        compacting_ = False;
   *        mutex_.unlock()
   *        bg_compaction_scheduled_ = False;
   *    }
   */
  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Is there a compaction running?
  /* 按我理解存在 bg_compaction_scheduled_ 为 false, 同时 compacting_ 为 true 的情况, 也即 compact 可能会
   * 被自发地运行. 但是结合了对 DBImpl::~DBImpl 的注释理解之后, 我觉得 compacting_ 的更新总是按照如下的流程:
   *    bg_compaction_scheduled_ = true;
   *    compacting_ = true;
   *    compacting_ = false;
   *    bg_compaction_scheduled_ = false;
   * 目测这个变量就是为了测试引入的, 因为我 grep 了一下正统代码里面没有使用过 compacting_.
   */
  bool compacting_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const Options& src);

}

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
