// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "include/db.h"
#include "include/env.h"
#include "include/status.h"
#include "include/table.h"
#include "include/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  // 这里一开始不理解为啥是 S <= smallest_snapshot, 我以为的是 S >= smallest_snapshot, 本来还以为 typo.
  // 具体参见 DoCompactionWork() 中解释.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction, 为啥不用 FileMetaData?
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  // outfile 表示着 compaction 生成的 sst 文件. builder 将内容写入到 outfile 中.
  WritableFile* outfile;
  TableBuilder* builder;

  // 我觉得这里是指本次 compact 输出的总字节数.
  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }  // 不知道 back() 么?

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

namespace {
class NullWritableFile : public WritableFile {
 public:
  virtual Status Append(const Slice& data) { return Status::OK(); }
  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }
};
}

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (*ptr > maxvalue) *ptr = maxvalue;
  if (*ptr < minvalue) *ptr = minvalue;
}
// 注意这里的实现细节.
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  // QA: 这些限制值有什么科学道理么?
  // A: 当我看完了 leveldb 所有实现之后, 我觉得这里就是随心而欲设置的值吧
  ClipToRange(&result.max_open_files,           20,     50000);
  ClipToRange(&result.write_buffer_size,        64<<10, 1<<30);
  ClipToRange(&result.large_value_threshold,    16<<10, 1<<30);
  ClipToRange(&result.block_size,               1<<10,  4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewWritableFile(InfoLogFileName(dbname),
                                        &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      // 日志文件都打开不了, 我觉得这里可以退出了.
      result.info_log = new NullWritableFile;
    }
  }
  return result;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : env_(options.env),
      internal_comparator_(options.comparator),
      options_(SanitizeOptions(dbname, &internal_comparator_, options)),
      owns_info_log_(options_.info_log != options.info_log),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      compacting_cv_(&mutex_),
      last_sequence_(0),
      mem_(new MemTable(internal_comparator_)),
      logfile_(NULL),
      log_(NULL),
      log_number_(0),
      bg_compaction_scheduled_(false),
      compacting_(false) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options.max_open_files - 10;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

/* 我之前一直在想会不会出现这种一种场景, 即某个线程 A 在使用着 dbimpl 执行一些操作, 然后另外一个线程 B 在执行
 * delete dbimpl, 导致 A 会出现 SIGSEGV? 现在我觉得只要用户保证在 delete dbimpl 时, 没有用户代码在其他线程
 * 使用着 dbimpl, 就不会出问题. leveldb 内部可能会在其他线程使用 dbimpl 的只有一种情况: BGWork(). 所以
 * 如下实现中当 bg_compaction_scheduled_ 为 true 时, 表明 BGWork() 在某个线程上运行着, 此时不能
 * delete dbimpl, 即需要等待直至 BGWork() 返回; 如果 bg_compaction_scheduled_ 为 false, 此时 leveldb 能
 * 保证自身没有在其他线程使用着 dbimpl, 此时只要用户也能保证没在其他线程使用着 dbimpl, 就可以确保 dbimpl 能被安全
 * delete.
 */
DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  // 这是唯一一处修改 shutting_down_ 的情况.
  // shutdown 或许没有必要用 release acquire 的语义, 另外我觉得析构函数阻塞不是很友好.
  // 这里由于 mutex lock 的存在, shutdown 还有必要用原子操作么? 参见 DoCompactionWork(), 会在不持有锁的情况下
  // 读取 shutting_down_.
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok

  /* 参见 MaybeScheduleCompaction(), leveldb 会原子地更新 bg_compaction_scheduled_, 以及调用
   * env_->Schedule(&DBImpl::BGWork, this). 因此当这样 bg_compaction_scheduled_ 为 true 时, 表明
   * BGWork() 已经被调度了, 此时等待其完成返回. 当 bg_compaction_scheduled_ 为 false 时, 表明当前没有
   * BGWork() 被调度, 而且由于这里同时更新了 shut down, 所以也不会再有 BGWork() 被调度, 所以这里可以直接返回.
   */
  if (bg_compaction_scheduled_) {
    while (bg_compaction_scheduled_) {
      bg_cv_.Wait();
    }
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  delete mem_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
}

Status DBImpl::NewDB() {
  assert(log_number_ == 0);
  assert(last_sequence_ == 0);

  /* descriptor 的初始内容.
   *
   * 参见 DB::Open(), VersionSet::LogAndApply() 可知, 再一次打开一个存在的 db 会创建一个新的 manifest 文件,
   * 该 manifest 文件的 file number 就是已经存在的 manifest 文件中保存的 next file number. 所以这里新建
   * manifest 的 file number 1, 其内保存的 next file number 为 2, 再一次打开该 db 会创建个 file number 为
   * 2 的 manifest, ...
   */
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(log_number_);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
    // SetCurrentFile 失败的话也应该 DeleteFile 的吧.
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

Status DBImpl::Install(VersionEdit* edit,
                       uint64_t new_log_number,
                       MemTable* cleanup_mem) {
  // QA: 这里为啥不直接使用 log_number_? 还要有参数传递?
  // A: 参见 Install() 的语义以及其使用场景.
  mutex_.AssertHeld();
  edit->SetLogNumber(new_log_number);
  edit->SetLastSequence(last_sequence_);
  return versions_->LogAndApply(edit, cleanup_mem);
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(env_, options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  versions_->CleanupLargeValueRefs(live, log_number_);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  LargeValueRef large_ref;
  FileType type;
  for (int i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &large_ref, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = (number == log_number_);
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          // 目测应该不存在 number > versions_->ManifestFileNumber() 的情况吧.
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:  // 目前 leveldb 中没用过 dbtmp 文件.
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kLargeValueFile:
          keep = versions_->LargeValueIsLive(large_ref);
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:  // 所以 old info log 不能被及时的删除咯.
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(env_, options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  // QA: committed only when 啥意思?
  // A: 我已经看完了 leveldb 所有实现, 仍然不知道这句话啥意思, 那就算了吧
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(&log_number_, &last_sequence_);
  if (s.ok()) {
    // Recover from the log file named in the descriptor
    SequenceNumber max_sequence(0);
    if (log_number_ != 0) {  // log_number_ == 0 indicates initial empty state
      s = RecoverLogFile(log_number_, edit, &max_sequence);
    }
    if (s.ok()) {
      last_sequence_ =
          last_sequence_ > max_sequence ? last_sequence_ : max_sequence;
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    WritableFile* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(env, info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  // checksum 有必要为 true 么? 先不说 table file 的内容被误修改的概率是多低, 在之前 paranoid checks 为
  // false 时那么宽松的政策之下还在乎这点错误的数据填充么?
  log::Reader reader(file, &reporter, true/*checksum*/);
  Log(env_, options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      // 当 paranoid_checks 为 true 时, 会由于该错误而终止 recovery. 否则忽略该 error.
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      delete mem;
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
    /* 这里按我理解是可以不 WriteLevel0Table() 而是把 mem 作为 dbimpl->memtable 的起始值.
     * 这里之所以 WriteLevel0Table() 可能是出于这么个场景考虑: 在上一次启动时, 由于磁盘满导致程序崩溃退出了, 然后
     * 程序开始重启, 然后 leveldb 开始 recovery log, 然后在这里的 WriteLevel0Table() 就能检测到磁盘已满,
     * 然后 DB::Open() 就会失败, 所以也就是 fast-fail.
     */
  }

  delete mem;
  delete file;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
  mutex_.AssertHeld();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  /* 我觉得这里没有必要 insert, 下面也没有必要 erase.
   * 因为这个函数是在 mutex lock 之后执行的, 所以在该函数执行期间, 外界是无法访问 pending_outputs_ 的...
   */
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(env_, options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);
  Status s = BuildTable(dbname_, env_, options_, table_cache_,
                        iter, &meta, edit);
  Log(env_, options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);
  return s;
}

Status DBImpl::CompactMemTable() {
  mutex_.AssertHeld();

  WritableFile* lfile = NULL;
  uint64_t new_log_number = versions_->NewFileNumber();

  VersionEdit edit;

  // Save the contents of the memtable as a new Table
  Status s = WriteLevel0Table(mem_, &edit);
  if (s.ok()) {
    s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
  }

  // Save a new descriptor with the new table and log number.
  if (s.ok()) {
    s = Install(&edit, new_log_number, mem_);
  }

  if (s.ok()) {
    // Commit to the new state
    mem_ = new MemTable(internal_comparator_);
    delete log_;
    delete logfile_;
    logfile_ = lfile;
    log_ = new log::Writer(lfile);
    log_number_ = new_log_number;
    // 我觉得 DeleteObsoleteFiles() 这么重的操作不必要放在 CompactMemTable() 中, 放到一个后台线程慢慢跑就行了
    // 么
    DeleteObsoleteFiles();
    MaybeScheduleCompaction();
  } else {
    delete lfile;
    env_->DeleteFile(LogFileName(dbname_, new_log_number));
  }
  return s;
}

/* 按我理解 TEST_CompactRange() 是用来测试 BackgroundCompaction 的性能, 那么其逻辑是不是也要与
 * BackgroundCompaction() 相符一点, 比如 BackgroundCompaction() 中的 fast path 这里是不是也要加上?
 */
void DBImpl::TEST_CompactRange(
    int level,
    const std::string& begin,
    const std::string& end) {
  MutexLock l(&mutex_);
  while (compacting_) {
    compacting_cv_.Wait();
  }
  Compaction* c = versions_->CompactRange(
      level,
      InternalKey(begin, kMaxSequenceNumber, kValueTypeForSeek),
      InternalKey(end, 0, static_cast<ValueType>(0)));

  if (c != NULL) {
    CompactionState* compact = new CompactionState(c);
    DoCompactionWork(compact);  // Ignore error in test compaction, 这样真的好么?
    CleanupCompaction(compact);
  }

  // Start any background compaction that may have been delayed by this thread
  MaybeScheduleCompaction();
}

Status DBImpl::TEST_CompactMemTable() {
  MutexLock l(&mutex_);
  return CompactMemTable();
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (compacting_) {  // 参见 compacting_ 的注释, 我觉得这里没有必要判断 compacting.
    // Some other thread is running a compaction.  Do not conflict with it.
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (!shutting_down_.Acquire_Load() &&
      /* 由于此时 bg_compaction_scheduled_ 为 true, 所以 compacting_ 一定为 false. 按我理解如果
       * compacting_ 为 true, 表明 BGWork() 已经在某个线程上运行着了, 所以触发本次 BackgroundCall()
       * 的 MaybeScheduleCompaction() 不应该执行 env->Schedule() 的, 即本次 BackgroundCall() 不应该运行,
       * 所以既然此时 BackgroundCall() 已经在运行了, 所以表明 compacting_ 为 false.
       */
      !compacting_) {
    BackgroundCompaction();
  }
  bg_compaction_scheduled_ = false;
  bg_cv_.SignalAll();

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
}

/* BackgroundCompaction() 大致分为三个阶段:
 * 1. 将 compact 需要的数据复制到 c 中; 按我理解这里复制是为了避免在执行 compact 操作时仍然持有锁.
 * 2. 执行 compact 操作;
 * 3. 执行一些收尾操作.
 */
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();
  Compaction* c = versions_->PickCompaction();
  if (c == NULL) {
    // Nothing to do
    return;
  }

  Status status;
  if (c->num_input_files(0) == 1 && c->num_input_files(1) == 0) {
    // Move file to next level. 真机智.
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = Install(c->edit(), log_number_, NULL);
    Log(env_, options_.info_log, "Moved #%lld to level-%d %lld bytes %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str());
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    CleanupCompaction(compact);
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down.
    // 按我理解, 这里没有必要吧? 我觉得总是应该输出一条 log, 同时更新 bg_error_.
  } else {
    Log(env_, options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
    if (options_.paranoid_checks && bg_error_.ok()) {
      bg_error_ = status;
    }
  }
}

/* 按我理解, 该函数应该由 DoCompactionWork() 内部调用, 不应该外界直接调用的.
 *
 * 当 DoCompactionWork() 成功返回时, 该函数就只是 delete compact;
 * 当 DoCompactionWork() 返回非 ok status 时, 该函数就依次清洗 CompactionState, 具体见实现.
 */
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (int i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);

    // 下面这些不应该放在 lock 内部.
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

// FinishCompactionOutputFile 依次更新 compact 每一个域, 可以参考 CompactionState 的定义一起看一下.
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable.
    // 首先我觉得这里没必要 check 的, 另外把 fill_cache 设置为 false 是不是好点?
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),output_number);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(env_, options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(env_, options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (int i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
    pending_outputs_.erase(out.number);
  }
  compact->outputs.clear();

  Status s = Install(compact->compaction->edit(), log_number_, NULL);
  if (s.ok()) {
    compact->compaction->ReleaseInputs();
    DeleteObsoleteFiles();
  } else {
    // Discard any files we may have created during this failed compaction
    // 之前已经把 compact->outputs CLEAR 了啊!
    for (int i = 0; i < compact->outputs.size(); i++) {
      env_->DeleteFile(TableFileName(dbname_, compact->outputs[i].number));
    }
  }
  return s;
}

/* 首先执行 compact 本身是不需要加锁的, 然后 DoCompactionWork() 调用时又是在加锁状态的, 所以
 * DoCompactionWork() 的大致结构如下:
 * 1. 趁着有锁这段时间做一下必须要有锁才能做的事, 比如更新 CompactionState::smallest_snapshot.
 * 2. 执行 compact;
 * 3. 执行收尾操作.
 */
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  Log(env_, options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = last_sequence_;
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  compacting_ = true;
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;

  /* 在了解这几个变量语义之前, 首先了解一下 input 值的模型, 对 input 进行遍历将产生如下序列:
   * (ukey0, seqN), (ukey0, seqN-1), ..., (ukey0, seq0), (ukey1, seqM), (ukey2, seqQ), ...
   *
   * has_current_user_key 标识着 current_user_key 中是否存在值, 若为 false, 则表明不存在; 否则存在.
   * current_user_key; 存放着当前正在处理的 user key 值, 如上 current_user_key 将依次为 ukey0, ukey1, ...
   * last_sequence_for_key; 在处理 current_user_key 存放的 user key 期间, 上一个被处理的, 具有相同 user
   * key 的 internal key 的 sequence 值. 如上在处理 ukey0 期间, last_sequence_for_key 将依次是:
   * kMaxSequenceNumber, seqN, seqN-1, ... seq0. 这里当 current_user_key 变更时, last_sequence_for_key
   * 总是会被置为 kMaxSequenceNumber. 按我理解是因为当 current_user_key 变更时, 上一个具有相同 user key 的
   * internal key 不存在, 另外假设其存在, 那么 last_sequence_for_key > current internal key sequence,
   * 所以这里将 last_sequence_for_key 置为了一个比 current internal key sequence 要大的值, 比如:
   * kMaxSequenceNumber.
   */
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {  // 有必要检测 shutting_down_ 么==
    // 1. 计算当前 input->key() 是否需要丢弃, 以及更新一些状态.
    // Handle key/value, add to state, etc.
    Slice key = input->key();
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      /* 此时表明 key 的格式不被 leveldb 理解, 我本来是以为这个会被丢弃的, 没想到 leveldb 是将 key 原样写入到
       * compact output 中.
       */
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }
      /* if (last_sequence_for_key > compact->smallest_snapshot) {
        if (ikey.sequence > compact->smallest_snapshot) {
          // 此时可能存在一个 snapshot, snapshot > smallest_snapshot, snapshot = ikey.sequence,
          // 当用户通过 snapshot 来读取 leveldb, 预期情况是读取到 ikey, 所以此时 drop 不能为 true.
        } else {  // ikey.sequence <= compact->smallest_snapshot
          if (ikey.type != kTypeDeletion) {
            // 此时 internal key 标识着一次 put 操作, 当用户通过 smallest snapshot 来读取 leveldb, 其
            // 期望读取到 ikey, 所以 ikey 不能被移除.
          } else {  // type == kTypeDeletion
            // 本来按照我的理解, 此时 ikey 不应该被删除. 因为如果 ikey 被移除了, 那么用户几乎需要读取完
            // leveldb 所有文件才能得到 ikey.user_key 不存在的结论; 反之如果 ikey 存在时, 那么当用户读取
            // 到 ikey 时就可以得知 ikey.user_key 被删除了, 不存在了. 但是 leveldb 这里还是删除了 ikey,
            // 大概是为了节省空间吧.

            if (!compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
              // IsBaseLevelForKey() 返回 false, 表明在 >= compact level + 2 层可能存在
              // (ikey.user_key(), seq0), 并且 seq0 < ikey.sequence.
              // 此时举个例子来表明不能将 drop 置为 true 的原因; 假设当前 compact level3, 4;
              // ikey.sequence = 10, smallest snapshot 为 10, 在 level7 中存在
              // (ikey.user_key(), 1); 那么这里如果 drop ikey, 会导致通过 smallest snapshot 读取
              // leveldb 时, 读取到 (ikey.user_key(), 1), 很显然这是错误的结果.
            } else {
              // IsBaseLevelForKey() 返回 true 表明 there is no data in higher levels(即 >=
              // compact->compaction->level_ + 2); 并且根据 Version::files_ 中的定理可以
              // 得出: data in lower levels will have larger sequence numbers; 并且根据上面的
              // 规则 (3) data in layers that are being compacted... 可以得出此时可以安全地将
              // drop 置为 true.
              drop = true;
            }
          }
        }
      } else {  // last_sequence_for_key <= compact->smallest_snapshot
        // 此时当用户通过 smallest_snapshot 读取 leveldb, 期望是读取到 prev internal key, 即
        // last_sequence_for_key 对应的 internal key, 所以此时 ikey 总是可以被移除.
        drop = true;
      } */

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(env_, options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeLargeValueRef, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // 2. 若 drop 为 true, 则丢弃, 调用 input Next() 方法处理下一个 key/value. 若 drop 为 false, 则写入
    // 新文件中.
    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      // 将未被丢弃的 key/value 写入 builder 中, 同时更新 output.
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);

      // 这里当 ParseInternalKey() 出错时, ikey 未被正确地初始化. 实验表明此时 ikey.type = 0(kTypeDeletion)
      // 所以此时很幸运地走向了 else 分支.
      if (ikey.type == kTypeLargeValueRef) {
        if (input->value().size() != LargeValueRef::ByteSize()) {  // 确实需要检查一下哈
          if (options_.paranoid_checks) {
            status = Status::Corruption("invalid large value ref");
            break;
          } else {  // 忽略本次 key/value.
            Log(env_, options_.info_log,
                "compaction found invalid large value ref");
          }
        } else {
          compact->compaction->edit()->AddLargeValueRef(
              LargeValueRef::FromRef(input->value()),
              compact->current_output()->number,
              input->key());
          compact->builder->Add(key, input->value());
        }
      } else {
        compact->builder->Add(key, input->value());
      }

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");  // 此时会丢弃本次 compaction 结果.
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {  // 这一步是不是可以提前一点, 毕竟可以省了一次 FinishCompactionOutputFile() 操作了.
    status = input->status();
  }
  delete input;
  input = NULL;

  mutex_.Lock();

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  compacting_ = false;
  compacting_cv_.SignalAll();
  return status;
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  mutex_.Lock();
  *latest_snapshot = last_sequence_;

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();  // 哦哦忘了 ref==
  internal_iter->RegisterCleanup(&DBImpl::Unref, this, versions_->current());
  // 如果我们这里 delete internal_iter; 那么就会死锁啊!
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  // TODO(opt): faster implementation
  Iterator* iter = NewIterator(options);
  iter->Seek(key);
  bool found = false;
  if (iter->Valid() && user_comparator()->Compare(key, iter->key()) == 0) {
    Slice v = iter->value();
    value->assign(v.data(), v.size());
    found = true;
  }
  // Non-OK iterator status trumps everything else
  Status result = iter->status();
  if (result.ok() && !found) {
    result = Status::NotFound(Slice());  // Use an empty error message for speed
  }
  delete iter;
  return result;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  SequenceNumber sequence =
      (options.snapshot ? options.snapshot->number_ : latest_snapshot);
  return NewDBIterator(&dbname_, env_,
                       user_comparator(), internal_iter, sequence);
}

void DBImpl::Unref(void* arg1, void* arg2) {
  DBImpl* impl = reinterpret_cast<DBImpl*>(arg1);
  Version* v = reinterpret_cast<Version*>(arg2);
  MutexLock l(&impl->mutex_);
  v->Unref();
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(last_sequence_);
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(s);
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Status status;

  WriteBatch* final = NULL;
  {
    MutexLock l(&mutex_);
    if (!bg_error_.ok()) {
      status = bg_error_;
    } else if (mem_->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = CompactMemTable();
    }
    if (status.ok()) {
      status = HandleLargeValues(last_sequence_ + 1, updates, &final);
    }
    if (status.ok()) {
      WriteBatchInternal::SetSequence(final, last_sequence_ + 1);
      last_sequence_ += WriteBatchInternal::Count(final);

      // Add to log and apply to memtable
      status = log_->AddRecord(WriteBatchInternal::Contents(final));
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(final, mem_);
      }
    }

    if (options.post_write_snapshot != NULL) {
      *options.post_write_snapshot =
          status.ok() ? snapshots_.New(last_sequence_) : NULL;
    }
  }
  if (final != updates) {
    delete final;
  }

  return status;
}

bool DBImpl::HasLargeValues(const WriteBatch& batch) const {
  if (WriteBatchInternal::ByteSize(&batch) >= options_.large_value_threshold) {
  // 这个 if 判断很精彩啊.
    for (WriteBatchInternal::Iterator it(batch); !it.Done(); it.Next()) {
      if (it.op() == kTypeValue &&
          it.value().size() >= options_.large_value_threshold) {
        return true;
      }
    }
  }
  return false;
}

// Given "raw_value", determines the appropriate compression format to use
// and stores the data that should be written to the large value file in
// "*file_bytes", and sets "*ref" to the appropriate large value reference.
// May use "*scratch" as backing store for "*file_bytes".
void DBImpl::MaybeCompressLargeValue(
    const Slice& raw_value,
    Slice* file_bytes,
    std::string* scratch,
    LargeValueRef* ref) {
  switch (options_.compression) {
    case kLightweightCompression: {
      port::Lightweight_Compress(raw_value.data(), raw_value.size(), scratch);
      if (scratch->size() < (raw_value.size() / 8) * 7) {
        // 大姐你这个就不能写成 (raw_value.size() * 7) / 8 么!
        *file_bytes = *scratch;
        *ref = LargeValueRef::Make(raw_value, kLightweightCompression);
        return;
      }

      // Less than 12.5% compression: just leave as uncompressed data
      break;
    }
    case kNoCompression:
      // Use default code outside of switch
      break;
  }
  // Store as uncompressed data
  *file_bytes = raw_value;
  *ref = LargeValueRef::Make(raw_value, kNoCompression);
}

Status DBImpl::HandleLargeValues(SequenceNumber assigned_seq,
                                 WriteBatch* updates,
                                 WriteBatch** final) {
  if (!HasLargeValues(*updates)) {
    // Fast path: no large values found
    *final = updates;
  } else {
    // Copy *updates to a new WriteBatch, replacing the references to
    *final = new WriteBatch;
    SequenceNumber seq = assigned_seq;
    for (WriteBatchInternal::Iterator it(*updates); !it.Done(); it.Next()) {
      switch (it.op()) {
        case kTypeValue:
          if (it.value().size() < options_.large_value_threshold) {
            (*final)->Put(it.key(), it.value());
          } else {
            std::string scratch;
            Slice file_bytes;
            LargeValueRef large_ref;
            MaybeCompressLargeValue(
                it.value(), &file_bytes, &scratch, &large_ref);
            InternalKey ikey(it.key(), seq, kTypeLargeValueRef);
            // 根据 RegisterLargeValueRef() 的返回值来判断 large_ref 对应的文件是否已经存在, 本来我想的是
            // 通过 open O_EXCL 标识来判断==
            //
            // 当 HandleLargeValues() 出错返回时, 已经 register 的 large ref 会在 log number 变更时被删除,
            // 比如当 dbimpl_->mem_ > 1mb 时, 此时会变更 log number, 同时移除这些 large ref.
            if (versions_->RegisterLargeValueRef(large_ref, log_number_,ikey)) {
              // TODO(opt): avoid holding the lock here (but be careful about
              // another thread doing a Write and changing log_number_ or
              // having us get a different "assigned_seq" value).
              /* 按我理解, 可以在 #1, #2 处添加相应代码来避免在磁盘写入期间持有锁. 本来还想着如果 avoid holding
               * lock 会不会导致多个线程在 Put same large value 时, 导致 large value file 被反复擦写, 导致
               * 线程可能会看到 large value file 只写入了一半这种情况? 但是原文实现是先写入到临时文件, 然后
               * Rename() 替换, 所以若 large value file 存在, 那么其一定是完整的内容.
               *
               * Q: 这里释放锁有一个问题就是目前 dbimpl->last_sequence_ 未被更新, 所以其他 thread doing a
               * Write 拿到的 last_sequence 就会有本线程重合, 这是一个问题. 但是原文的 changing log_number_
               * or having us get a ... 没看懂==
               */

              uint64_t tmp_number = versions_->NewFileNumber();
              pending_outputs_.insert(tmp_number);
              // #1 mutex_.unlock()
              std::string tmp = TempFileName(dbname_, tmp_number);
              WritableFile* file;
              Status s = env_->NewWritableFile(tmp, &file);
              if (!s.ok()) {
                return s;     // Caller will delete *final
              }

              file->Append(file_bytes);
              // QA: 大哥这里不调用 file->Sync() 么!!!
              // A: 最新 leveldb 已经移除了对 large value 的支持, 心好痛==.
              s = file->Close();
              delete file;

              if (s.ok()) {
                const std::string fname =
                    LargeValueFileName(dbname_, large_ref);
                s = env_->RenameFile(tmp, fname);
              } else {
                Log(env_, options_.info_log, "Write large value: %s",
                    s.ToString().c_str());
              }
              // #2 mutex_.lock()
              pending_outputs_.erase(tmp_number);

              if (!s.ok()) {
                env_->DeleteFile(tmp);  // Cleanup; intentionally ignoring error
                return s;     // Caller will delete *final
              }
            }

            // Put an indirect reference in the write batch in place
            // of large value
            WriteBatchInternal::PutLargeValueRef(*final, it.key(), large_ref);
          }
          break;
        case kTypeLargeValueRef:
          return Status::Corruption("Corrupted write batch");
          break;
        case kTypeDeletion:
          (*final)->Delete(it.key());
          break;
      }
      seq = seq + 1;
    }
  }
  return Status::OK();
}

bool DBImpl::GetProperty(const Slice& property, uint64_t* value) {
  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level < 0 || level >= config::kNumLevels) {
      return false;
    } else {
      *value = versions_->NumLevelFiles(level);
      return true;
    }
  }
  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }


/* DB::Open() 依次执行一揽子操作, 当这些操作全都执行成功, 就表明 open 成功. 当某个操作执行失败时, 都是导致 open
 * 出错并返回. 具体执行的操作列表参见源码.
 */
Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  // 没必要 lock 吧? 毕竟这时候 impl 还没有暴露出去呢!
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    // log number 的分配.
    impl->log_number_ = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, impl->log_number_),
                                     &lfile);
    if (s.ok()) {
      impl->logfile_ = lfile;
      impl->log_ = new log::Writer(lfile);
      s = impl->Install(&edit, impl->log_number_, NULL);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  Status result = env->LockFile(LockFileName(dbname), &lock);
  if (result.ok()) {
    uint64_t number;
    LargeValueRef large_ref;
    FileType type;
    for (int i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &large_ref, &type)) {
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(LockFileName(dbname));
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}
