// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "include/db.h"

// Q: 并不清楚 snapshot 是如何实现的?!

namespace leveldb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each Snapshot corresponds to a particular sequence number.
// Snapshot 本质上是双链表中的节点.
class Snapshot {
 public:
  SequenceNumber number_;  // const after creation

 private:
  friend class SnapshotList;

  // Snapshot is kept in a doubly-linked circular list
  Snapshot* prev_;
  Snapshot* next_;

  // 按我理解, leveldb 内部根据 list_ 值来判断 Snapshot 实例是 leveldb 构造的, 还是用户手动构造的.
  // 参见 SnapshotList->New().
  SnapshotList* list_;                 // just for sanity checks
};

// 类似 util/cache.cc 中 LRUCache 使用的双链表结构.
class SnapshotList {
 public:
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
  }

  bool empty() const { return list_.next_ == &list_; }
  // 注意 oldest, newest 的顺序.
  Snapshot* oldest() const { assert(!empty()); return list_.next_; }
  Snapshot* newest() const { assert(!empty()); return list_.prev_; }

  // 类似 std::list::append().
  const Snapshot* New(SequenceNumber seq) {
    Snapshot* s = new Snapshot;
    s->number_ = seq;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    return s;
  }

  // 类似 std::list::remove();
  void Delete(const Snapshot* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    delete s;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  Snapshot list_;
};

}

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
