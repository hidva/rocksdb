// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "include/env.h"
#include "include/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

/* 在看该类时要时刻脑补着 leveldb 数据库中数据的模型:
 * (ukey0, seq0), (ukey0, seq1), ..., (ukey1, seq2), ...
 */
// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {
 public:
  DBIter(const std::string* dbname, Env* env,
         const Comparator* cmp, Iterator* iter, SequenceNumber s)
      : dbname_(dbname),
        env_(env),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        large_(NULL),
        valid_(false) {
  }
  virtual ~DBIter() {
    delete iter_;
    delete large_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    return key_;
  }
  virtual Slice value() const {
    assert(valid_);
    if (large_ == NULL) {
      return value_;
    } else {
      MutexLock l(&large_->mutex);
      if (!large_->produced) {
        ReadIndirectValue();
      }
      // 你说这是图啥啊, 非得延迟读取, 这时候如果 ReadIndirectValue() 出错了, 用户都不能及时知道!
      return large_->value;
    }
  }

  virtual void Next() {
    assert(valid_);
    // iter_ is already positioned past DBIter::key()
    FindNextUserEntry();
  }

  virtual void Prev() {
    assert(valid_);
    bool ignored;
    ScanUntilBeforeCurrentKey(&ignored);
    FindPrevUserEntry();
  }

  virtual void Seek(const Slice& target) {
    ParsedInternalKey ikey(target, sequence_, kValueTypeForSeek);
    std::string tmp;
    AppendInternalKey(&tmp, ikey);
    iter_->Seek(tmp);
    FindNextUserEntry();
  }
  virtual void SeekToFirst() {
    iter_->SeekToFirst();
    FindNextUserEntry();
  }

  virtual void SeekToLast();

  virtual Status status() const {
    if (status_.ok()) {
      if (large_ != NULL && !large_->status.ok()) return large_->status;
      return iter_->status();
    } else {
      return status_;
    }
  }

 private:
  void FindNextUserEntry();
  void FindPrevUserEntry();
  void SaveKey(const Slice& k) { key_.assign(k.data(), k.size()); }
  void SaveValue(const Slice& v) {
    if (value_.capacity() > v.size() + 1048576) {
      // 图啥啊, 多用你 1048576 字节怎么了!
      std::string empty;
      swap(empty, value_);
    }
    value_.assign(v.data(), v.size());
  }
  bool ParseKey(ParsedInternalKey* key);
  void SkipPast(const Slice& k);
  void ScanUntilBeforeCurrentKey(bool* found_live);

  void ReadIndirectValue() const;

  /* DBIter 实现了延迟打开 large value. 即对于一个 large value, 仅当用户调用了 iter->value() 时才会打开
   * large value file, 读取并解压其内容.
   *
   * produced 若为真, 则表明 value 中存放着 large value 实际内容. 若为 false, 则表明 value 中存放了
   * LargeValueRef::data.
   *
   * status 用来表明打开 large value file, 读取并解压其内容期间有没有出错.
   *
   * mutex 用来保护其他所有成员.
   *
   * 按我理解这里不需要使用 mutex 的啊! 因为 DBIter 自身不是线程安全的, 所以同一个 iter 对象的 value() 不会
   * 同时被多个线程中调用.
   */
  struct Large {
    port::Mutex mutex;
    std::string value;
    bool produced;
    Status status;
  };

  const std::string* const dbname_;
  Env* const env_;

  const Comparator* const user_comparator_;

  // 不变量00: iter_ is positioned just past current entry for DBIter if valid_
  // 由 dbimpl NewInternalIterator() 分配.
  Iterator* const iter_;

  SequenceNumber const sequence_;
  Status status_;
  std::string key_;                  // Always a user key
  // large_, value_ 保存了 key_ 对应的值. 若 large_ 为 NULL, 则表明 key_ 对应的值存放在 value_ 中. 若
  // large_ 不为 NULL, 则表明 key_ 是一个 large value key.
  std::string value_;
  Large* large_;      // Non-NULL if value is an indirect reference
  // 若为真, 则表明当前 key_ 有意义. 否则无意义.
  bool valid_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

// parse iter->key(), 并将结果写入 ikey 中. 返回 true/false 表明解析结果.
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

/* 该函数会一直 next iter_, 直至 iter_ 不再 valid; 或者找到了一个在 sequence_ 下存在的 user_key, 此时会设置
 * key_, value_, valid_ 等变量, 并使用 SkipPast() 来调整 iter_.
 */
void DBIter::FindNextUserEntry() {
  if (large_ != NULL) {
    // 如果 large_ 不为 NULL, 那么这里需要 delete, 并且如果上一个 large 在读取 large file 时出错那么这里还需要
    // 保存一下出错状态.
    // 这里不对 large 进行加锁之后才获取其 status 么.
    if (status_.ok() && !large_->status.ok()) {
      status_ = large_->status;
    }
    delete large_;
    large_ = NULL;
  }
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      // Skip past corrupted entry. 我的想法是此时终止遍历.
      iter_->Next();
      continue;
    }
    if (ikey.sequence > sequence_) {
      // Ignore entries newer than the snapshot
      iter_->Next();
      continue;
    }

    switch (ikey.type) {
      case kTypeDeletion:
        SaveKey(ikey.user_key);  // Make local copy for use by SkipPast()
        iter_->Next();
        SkipPast(key_);
        // Do not return deleted entries.  Instead keep looping.
        break;

      case kTypeValue:
        SaveKey(ikey.user_key);
        SaveValue(iter_->value());
        iter_->Next();
        SkipPast(key_);
        // Yield the value we just found.
        valid_ = true;
        return;

      case kTypeLargeValueRef:
        SaveKey(ikey.user_key);
        // Save the large value ref as value_, and read it lazily on a call
        // to value()
        SaveValue(iter_->value());
        large_ = new Large;
        large_->produced = false;
        iter_->Next();
        SkipPast(key_);
        // Yield the value we just found.
        valid_ = true;
        return;
    }
  }
  valid_ = false;
  key_.clear();
  value_.clear();
  assert(large_ == NULL);
}

// 当该函数返回时, iter_ 要么为 !Valid(), 要么 iter->key().userkey() != k;
void DBIter::SkipPast(const Slice& k) {
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    // Note that if we cannot parse an internal key, we keep looping
    // so that if we have a run like the following:
    //     <x,100,v> => value100
    //     <corrupted entry for user key x>
    //     <x,50,v> => value50
    // we will skip over the corrupted entry as well as value50.
    // 就怕当 corrupted entry for user key x 也污染了 value50 咯~
    if (ParseKey(&ikey) && user_comparator_->Compare(ikey.user_key, k) != 0) {
      break;
    }
    iter_->Next();
  }
}

void DBIter::SeekToLast() {
  // Position iter_ at the last uncorrupted user key and then
  // let FindPrevUserEntry() do the heavy lifting to find
  // a user key that is live.
  iter_->SeekToLast();
  ParsedInternalKey current;
  while (iter_->Valid() && !ParseKey(&current)) {
    iter_->Prev();
  }
  if (iter_->Valid()) {
    SaveKey(current.user_key);
  }
  FindPrevUserEntry();
}

// Let X be the user key at which iter_ is currently positioned.
// Adjust DBIter to point at the last entry with a key <= X that
// has a live value.
void DBIter::FindPrevUserEntry() {
  // Consider the following example:
  //
  //     A@540
  //     A@400
  //
  //     B@300
  //     B@200
  //     B@100        <- iter_
  //
  //     C@301
  //     C@201
  //
  // The comments marked "(first iteration)" below relate what happens
  // for the preceding example in the first iteration of the while loop
  // below.  There may be more than one iteration either if there are
  // no live values for B, or if there is a corruption.
  /* 在每一次循环开始之前, iter_ 要么 Valid() == false; 要么 Valid == true, 此时
   * key_ = iter_->key().userkey().
   *
   * FindPrevUserEntry() 会首先使用 ScanUntilBeforeCurrentKey() 来判断 key_ 在 sequence 下是否存在; 如果
   * 存在则使用 FindNextUserEntry() 来更新 key_, value_ 等变量; 若不存在, 则使用
   * ScanUntilBeforeCurrentKey() 发现的比 key_ 更小的 key 再一次尝试.
   */
  while (iter_->Valid()) {
    std::string saved = key_;
    bool found_live;
    ScanUntilBeforeCurrentKey(&found_live);
    // (first iteration) iter_ at A@400
    if (found_live) {
      // Step forward into range of entries with user key >= saved
      if (!iter_->Valid()) {
        iter_->SeekToFirst();
      } else {
        iter_->Next();
      }
      // 此时 iter->Valid() 为 true.
      // (first iteration) iter_ at B@300

      FindNextUserEntry();  // Sets key_ to the key of the next value it found
      if (valid_ && user_comparator_->Compare(key_, saved) == 0) {
        // (first iteration) iter_ at C@301
        return;
      }
      // 我觉得不可能走到这里的!

      // FindNextUserEntry() could not find any entries under the
      // user key "saved".  This is probably a corruption since
      // ScanUntilBefore(saved) found a live value.  So we skip
      // backwards to an earlier key and ignore the corrupted
      // entries for "saved".
      //
      // (first iteration) iter_ at C@301 and saved == "B"
      key_ = saved;  // 此时 key_ 的状态不定, 所以将其设置为一个确定性的状态.
      bool ignored;
      ScanUntilBeforeCurrentKey(&ignored);
      // (first iteration) iter_ at A@400
    }
  }
  valid_ = false;
  key_.clear();
  value_.clear();
}

/* 另 savedkey = key_. 当该函数 return 时, 此时 iter_ 要么 !Valid(); 要么 iter_->key().userkey() 在
 * sequence_ 下存在, 而且其小于 savedkey, 此时会将其修改为 key_ 的值.
 *
 * 当该函数返回时, 若 found_live 为 true, 则表明 savedkey 在 sequence_ 下存在; 若为 false, 则表明不存在.
 */
void DBIter::ScanUntilBeforeCurrentKey(bool* found_live) {
  *found_live = false;
  // QA: 不清楚为啥要来这么一出?
  // A: 参见 Prev() 实现, 由于不变量00 的存在, 当 valid_ 为 true, key_ 为当前数据库中最后一个元素时, 此时
  // iter_ 处于最后一个元素的下一个位置, 类似 std::vector::end 的存在, 所以此时需要调整 iter_.
  if (!iter_->Valid()) {
    iter_->SeekToLast();
  }

  while (iter_->Valid()) {
    ParsedInternalKey current;
    if (!ParseKey(&current)) {
      iter_->Prev();
      continue;
    }

    if (current.sequence > sequence_) {
      // Ignore entries that are serialized after this read
      iter_->Prev();
      continue;
    }

    const int cmp = user_comparator_->Compare(current.user_key, key_);
    if (cmp < 0) {
      SaveKey(current.user_key);
      return;
    } else if (cmp == 0) {
      switch (current.type) {
        case kTypeDeletion:
          *found_live = false;
          break;

        case kTypeValue:
        case kTypeLargeValueRef:
          *found_live = true;
          break;
      }
    } else {  // cmp > 0
      // 此时可以证明 found_live 一定为 false, 所以这里没有必要再一次设置为 false.
      *found_live = false;
    }

    iter_->Prev();
  }
}

void DBIter::ReadIndirectValue() const {
  assert(!large_->produced);
  large_->produced = true;
  LargeValueRef large_ref;
  if (value_.size() != LargeValueRef::ByteSize()) {
    large_->status = Status::Corruption("malformed large value reference");
    return;
  }
  memcpy(large_ref.data, value_.data(), LargeValueRef::ByteSize());
  std::string fname = LargeValueFileName(*dbname_, large_ref);
  // 为啥不使用 ReadFileToString() 来读取呢?
  RandomAccessFile* file;
  Status s = env_->NewRandomAccessFile(fname, &file);
  if (s.ok()) {
    uint64_t file_size = file->Size();
    uint64_t value_size = large_ref.ValueSize();
    large_->value.resize(value_size);
    Slice result;
    s = file->Read(0, file_size, &result,
                   const_cast<char*>(large_->value.data()));
    if (s.ok()) {
      if (result.size() == file_size) {
        switch (large_ref.compression_type()) {
          case kNoCompression: {
            if (result.data() != large_->value.data()) {
              large_->value.assign(result.data(), result.size());
            }
            break;
          }
          case kLightweightCompression: {
            std::string uncompressed;
            if (port::Lightweight_Uncompress(result.data(), result.size(),
                                       &uncompressed) &&
                uncompressed.size() == large_ref.ValueSize()) {
              swap(uncompressed, large_->value);
            } else {
              s = Status::Corruption(
                  "Unable to read entire compressed large value file");
            }
          }
        }
      } else {
        s = Status::Corruption("Unable to read entire large value file");
        // 大哥你就不能再一次 read 么.
      }
    }
    delete file;        // Ignore errors on closing
  }
  if (!s.ok()) {
    large_->value.clear();
    large_->status = s;
  }
}

}  // anonymous namespace

Iterator* NewDBIterator(
    const std::string* dbname,
    Env* env,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    const SequenceNumber& sequence) {
  return new DBIter(dbname, env, user_key_comparator, internal_iter, sequence);
}

}
