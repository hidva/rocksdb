// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;
}

namespace log { class Writer; }

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;


// Version, 参见 version.README.md
class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // 这个我觉得应该是说 this version 应该经过了 VersionSet::Finalize 处理, 因为 Finalize() 中会进行
  // SortLevel, 并且 AddIterators 默认了 Version 各层是有序排列的了.
  /*
   * 这里 the contents of this Version 是指当前 Version 实例各个 level 级别下的 table file 集合. 即:
   *    MergingIterator iter(comparator, iters->data(), iters->size());
   * 此时 iter 产生的结果与 Table->NewIterator() 产生的结果相同, iter 将按照 key 从小到大的方式遍历所有 level
   * 下所有 table file.
   */
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  /* 按我理解, Version 依附于 VersionSet, VersionSet 负责 Version 的分配构造以及析构回收.
   * Ref() 增加当前 Version 的引用计数避免被 VersionSet 回收.
   * Unref() 减少对 Version 的引用计数, 同时通知 VersionSet 可能需要回收当前 Version.
   */
  void Ref();
  void Unref();

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  /*
   * 生成的迭代器负责迭代 flist_, 其中 flist_ = files_[level], flist_ 的语义参考 LevelFileNumIterator 的
   * 实现. 其返回的迭代器等同于 Table->NewIterator(), 区别在于当一个 table 文件迭代完毕之后, 会跳过下一个 table
   * 文件继续迭代.
   */
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  VersionSet* vset_;            // VersionSet to which this Version belongs
  Version* next_;               // Next version in linked list
  int refs_;                    // Number of live refs to this version
  /* cleanup_mem_ 是当前 Version 某个 level 0 table 文件在内存中的存在形式. 参见 MaybeDeleteOldVersions()
   * 的实现来了解 cleanup mem 存在的意义.
   */
  MemTable* cleanup_mem_;       // NULL, or table to delete when version dropped

  // List of files per level
  /* leveldb 中与 files_ 相关的几条定理:
   * 1. 如果在 L 层存在 (ukey, seq), 那么在任意 l 层, l < L, 找不到 (ukey, seq0), seq0 < seq.
   * 2. 如果 ukey 在每一层都存在, 且对应 seq 分别是: seq1, seq2, ..., seqMAXLEVEL; 那么 seq1 > seq2 >
   *    ... > seqMAXLEVEL. 如果 ukey 在 dbimpl memtable 与 level 0 中均存在, 那么 seqMEMTABLE > seq0;
   *
   * 定理 1 的证明: 假设存在 (ukey, seq0), 那么在 (ukey, seq) 经过一次次 compact 从 level0 下降到 L 层的过程
   * 中, 其势必会读取到 (ukey, seq0), 并且也会把 (ukey, seq0) 也带到 L 层中, 所以不存在这样的 (ukey, seq0).
   * 好吧这并不是一个很严谨的证明==
   *
   * 定理 2 的证明: 根据定理 1 可以证明不存在 level N, M, N < M, 并且 seqN < seqM.
   */
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize(). 关于 compaction score 是如何计算的, 参见 VersionSet::Finalize(),
  // 和我最开始想的一样: level 当前总大小 / level 允许的最大大小.
  double compaction_score_;
  int compaction_level_;

  explicit Version(VersionSet* vset)
      : vset_(vset), next_(NULL), refs_(0),
        cleanup_mem_(NULL),
        compaction_score_(-1),
        compaction_level_(-1) {
  }

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};


// VersionSet, 参见 version.README.md.
class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Iff Apply() returns OK, arrange to delete
  // cleanup_mem (if cleanup_mem != NULL) when it is no longer needed
  // by older versions.
  Status LogAndApply(VersionEdit* edit, MemTable* cleanup_mem);

  // Recover the last saved descriptor from persistent storage. 我怎么觉得应该在 VersionSet 的构造函数
  // 中调用一下该函数呢? 不然如果外界忘记调用 Recover() 那么他们将使用着错误的结果吧.
  Status Recover(uint64_t* log_number, SequenceNumber* last_sequence);

  // Save current contents to *log
  // 参见实现了解具体写入了哪些内容.
  Status WriteSnapshot(log::Writer* log);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  // QA: 为什么 manifest_file_number_ 一直保持不变. 难道不该像 NewFileNumber() 这样么?
  // A: 参见 manifest_file_number_ 的文档.
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number;
  // QA: 难道不得生成并写入个 VersionEdit 来表明变更么.
  // A: VersionSet 会在合适的时候写入个 VersionEdit 来持久化变更信息, 参见 LogAndApply().
  // 任何调用方在实际使用返回的 file number 之前总应该调用 LogAndApply() 来持久化变更后的 next file number.
  // 不然就会导致持久化设备中记录的仍是 old next_file_number. 可以参考 DB::Open() 打开 log 这一段代码
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Return the number of Table files at the specified level. 基于 current version.
  int NumLevelFiles(int level) const;

  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  // 本来我以为 PickCompaction() 会调用 CompactRange() 的, 没想到没有.
  // 另外按照我的理解, CompactRange() 之所以没有像 PickCompaction() 那样尝试 grow the number of inputs,
  // 是因为 CompactRange() 是指定对 [begin, end] 进行 compact. 如果进行了尝试, 可能会导致最终 compact 的区间
  // 不是精准的 [begin, end].
  Compaction* CompactRange(
      int level,
      const InternalKey& begin,
      const InternalKey& end);

  // Create an iterator that reads over the compaction inputs(inputs_[0], inputs_[1]) for "*c".
  // 基本思路与 Version::AddIterators() 差不多.
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const { return current_->compaction_score_ >= 1; }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  // 根据实现, 目前是把 VersionSet 中所有 version, 所有 file 都 add 到 *live 中. 并且貌似没有更改 internal
  // state 啊.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  /* 按我理解这里是求取 key 在 v 中的大概偏移值. 但现在有一个问题, v.files_ 是 '二维' 的, 那如何求取偏移呢?
   * 根据代码实现结合自身理解, 这里是把 v.files_ '挤压拍平' 成一维之后, 再求取偏移的. 如: v 有 2 层,
   * v.files_[0] 存放着 [ka, kb], [kc, kd]; v.files_[1] 存放着 [ke, kf], [kg, kh]; 其中 ka < kb < ke <
   * kf < kc < kd < kg < kh; kg < k < kh, 那么 k 在 v 中的偏移就是 filesize(ka, kb) +
   * filesize(kc, kd) + filesize(ke, kf) + (k 在 file(kg, kh) 中的偏移).
   */
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Register a reference to a large value with the specified
  // large_ref from the specified file number.  Returns "true" if this
  // is the first recorded reference to the "large_ref" value in the
  // database, and false otherwise.
  bool RegisterLargeValueRef(const LargeValueRef& large_ref,
                             uint64_t filenum,
                             const InternalKey& internal_key);

  // Cleanup the large value reference state by eliminating any
  // references from files that are not includes in either "live_tables"
  // or "log_file".
  // 根据 impl 文档, 当 large value ref 不被 live tables, log file 引用时, 那么这个 large value 就可以被
  // 安全删除掉了.
  void CleanupLargeValueRefs(const std::set<uint64_t>& live_tables,
                             uint64_t log_file_num);

  // Returns true if a large value with the given reference is live.
  bool LargeValueIsLive(const LargeValueRef& large_ref);

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  // 按我理解, Finalize 有固化 v 的意思, 经过 Finalize() 之后, v 将只读.
  Status Finalize(Version* v);

  // Delete any old versions that are no longer needed. 很显然此时是否 needed 是根据 ref 来判断的.
  void MaybeDeleteOldVersions();

  struct BySmallestKey;
  Status SortLevel(Version* v, uint64_t level);

  void GetOverlappingInputs(
      int level,
      const InternalKey& begin,
      const InternalKey& end,
      std::vector<FileMetaData*>* inputs);

  // *smallest = min(all of inputs.smallest); *larget = max(all of inputs.largest).
  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;

  /* next_file_number_ 指定了下一个 table file 的 number.
   * manifest_file_number_, 按我理解 A new MANIFEST file (with a new number embedded in the file
   * name) is created whenever the database is reopened, 然后就会一直使用这个 manifest 文件不再会创建新的
   * manifest 文件. manifest_file_number_ 指定了当前使用着的 manifest 文件的序号.
   */
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;

  // Opened lazily
  // 关于 descriptor 是什么, 参见 vesion.README.md; descriptor_file_ 就是传说中的 manifest 文件.
  // 两者要么都为 Null, 要么都不为 Null, descriptor_file_ 是 descriptor_log_ 的底层存储.
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;

  // Versions are kept in a singly linked list that is never empty
  Version* current_;    // Pointer to the last (newest) list entry
  Version* oldest_;     // Pointer to the first (oldest) list entry

  // Map from large value reference to the set of <file numbers,internal_key>
  // values containing references to the value.  We keep the
  // internal key as a std::string rather than as an InternalKey because
  // we want to be able to easily use a set. 很显然如果使用 InternalKey, 需要重载 operator< 等运算符.
  // 根据 CleanupLargeValueRefs() 可知, 这里 File number 可能是 table file, 也可能是 log file.
  //
  // 之前纳闷为啥要把 larger value ref 也作为 descriptor 的一部分, 后来在 DeleteObsoleteFiles() 中发现, 如果
  // 不这么做如何很效率地确定 large value file 是否可以被删除呢?
  typedef std::set<std::pair<uint64_t, std::string> > LargeReferencesSet;
  typedef std::map<LargeValueRef, LargeReferencesSet> LargeValueMap;
  LargeValueMap large_value_refs_;

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Add all inputs to this compaction as delete operations to *edit.
  // 这里为啥还需要传入个 edit, 直接使用 edit_ 不更符合语义, 毕竟 edit_ holds the edits to ...
  void AddInputDeletions(VersionEdit* edit);

  // QA: 语义, 实现都没有读懂!
  // A: 已然了然于心.
  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  /* 当返回 true 时, 表明在任何 level( > L + 1) 层不再存在 user_key. 若返回 false, 则表明**可能**存在.
   * 关于 IsBaseLevelForKey 的使用场景, 参考 DoCompactionWork().
   */
  bool IsBaseLevelForKey(const Slice& user_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  explicit Compaction(int level);

  // 参见相应的 getter 成员函数来了解语义.
  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  /* QA: 按我理解, inputs_[0] 存放着 level_ 层需要 compact 的文件, inputs[1] 存放着
   * level_ + 1 层需要 compact 的文件, 即 inputs_ 不关心需要 compact 的文件是如何选取的. 参见 impl 的说法,
   * 针对 level0 进行 compact 会选取 level0 所有文件, 针对 level-L(L > 0) 的 compact 会只选择一个文件.
   * A: 貌似真的是这样.
   */
  // Each compaction reads inputs from "level_" and "level_+1"
  // 根据 GetOverlappingInputs() 的实现细节, inputs_ 是否符合 LevelFileNumIterator 中 flist 的参数说明
  // 依赖于 input_version 各个 level 是否符合 flist 的参数说明. 目前由于 input_version 都经过了 Finalize()
  // 的处理, 所以 input_version 各个 level 都符合 flist 的参数说明.
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State for implementing IsBaseLevelForKey

  // QA: 这里 input_version_->levels_ 应该是指 input_version_->files_
  // 这个变量干啥用的啊?
  // A: 指定了在 IsBaseLevelForKey(ukey) 中, 在各个 level(level >= level_ + 2)上搜索 ukey 是否可能存在时
  // 的搜索起点, 即搜索将从下标 level_ptrs_[level] 开始, 而不总是从 0 开始. 之所以存在该变量的原因是因为外界
  // (目测就 DoCompactionWork() 一个调用者)总是按照 ukey 从小到大的顺序调用 IsBaseLevelForKey():
  // IsBaseLevelForKey(ukey1), IsBaseLevelForKey(ukey2), ... 此时 ukey1 < ukey2. 所以如果我们在
  // IsBaseLevelForKey(ukey1) 时发现 input_version_->files_[level][idx].largest < ukey1, 那么在
  // IsBaseLevelForKey(ukey2) 时, 我们总是可以跳过 idx, 而从 input_version_->files_[level][idx + 1]
  // 开始搜索起. 所以 level_ptrs_ 就保存着这么个信息, 每次调用 IsBaseLevelForKey() 都会更新 level_ptrs_.
  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  int level_ptrs_[config::kNumLevels];
};

}

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
