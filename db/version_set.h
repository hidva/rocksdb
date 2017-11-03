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


// Version 严重依赖 VersionSet, 所以想要了解 Version, 需要先了解 VersionSet.
class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  /*
   * 这里 the contents of this Version 是指当前 Version 实例各个 level 级别下的 table file 集合. 即:
   *    MergingIterator iter(comparator, iters->data(), iters->size());
   * 此时 iter 产生的结果与 Table->NewIterator() 产生的结果相同, iter 将按照 key 从小到大的方式遍历所有 level
   * 下所有 table file.
   */
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
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
  MemTable* cleanup_mem_;       // NULL, or table to delete when version dropped

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
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
  // Q: 不懂其意.
  Status LogAndApply(VersionEdit* edit, MemTable* cleanup_mem);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(uint64_t* log_number, SequenceNumber* last_sequence);

  // Save current contents to *log
  // 参见实现了解具体写入了哪些内容.
  Status WriteSnapshot(log::Writer* log);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  // Q: 为什么 manifest_file_number_ 一直保持不变. 难道不该像 NewFileNumber() 这样么?
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
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
  Compaction* CompactRange(
      int level,
      const InternalKey& begin,
      const InternalKey& end);

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const { return current_->compaction_score_ >= 1; }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
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
  void CleanupLargeValueRefs(const std::set<uint64_t>& live_tables,
                             uint64_t log_file_num);

  // Returns true if a large value with the given reference is live.
  bool LargeValueIsLive(const LargeValueRef& large_ref);

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  Status Finalize(Version* v);

  // Delete any old versions that are no longer needed.
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

  // Q: 用在哪一个地方? 按我理解, next_file_number_ 指定了下一个 table file 的 number, 而
  // manifest_file_number 指定了下一个 manifest file 的 Number.
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;

  // Opened lazily
  // Q: descriptor 是指什么? manifest 么?
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;

  // Versions are kept in a singly linked list that is never empty
  Version* current_;    // Pointer to the last (newest) list entry
  Version* oldest_;     // Pointer to the first (oldest) list entry

  // Map from large value reference to the set of <file numbers,internal_key>
  // values containing references to the value.  We keep the
  // internal key as a std::string rather than as an InternalKey because
  // we want to be able to easily use a set. 很显然如果使用 InternalKey, 需要重载 operator< 等运算符.
  // Q: 为啥要保存这么些东西?
  typedef std::set<std::pair<uint64_t, std::string> > LargeReferencesSet;
  typedef std::map<LargeValueRef, LargeReferencesSet> LargeValueMap;
  LargeValueMap large_value_refs_;

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // Q: 为何需要这么个指针, 难道每次不是随机的么? 我好像知道了 VersionEdit->compact_pointers_ 的用处了, 就是
  // 为了持久化这个数组啊. 但是为啥要保存 compact pointer, 本来我以为每次 compact 都是随机选择呢.
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
  // Q: 这里为啥还需要传入个 edit, 直接使用 edit_ 不更符合语义, 毕竟 edit_ holds the edits to ...
  void AddInputDeletions(VersionEdit* edit);

  // Q: 语义, 实现都没有读懂!
  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
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

  /* Q: 按我理解, inputs_[0] 存放着 level_ 层需要 compact 的文件, inputs[1] 存放着
   * level_ + 1 层需要 compact 的文件, 即 inputs_ 不关心需要 compact 的文件是如何选取的. 参见 impl 的说法,
   * 针对 level0 进行 compact 会选取 level0 所有文件, 针对 level-L(L > 0) 的 compact 会只选择一个文件.
   */
  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State for implementing IsBaseLevelForKey

  // Q: 这里 input_version_->levels_ 应该是 input_version_->files_
  // 这个变量干啥用的啊?
  //
  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  int level_ptrs_[config::kNumLevels];
};

}

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
