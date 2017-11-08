// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "include/db.h"
#include "include/env.h"
#include "include/iterator.h"

namespace leveldb {

/* BuildTable 逻辑, 按我理解, 明明是线性的逻辑, 原文七扭八拗:
 * 1. 创建 SST 文件 filename.
 * 2. 遍历 iter, 将 key, value 写入 SST 文件中, 更新 filemeta, 以及 version edit.
 * 3. 测试迭代器的状态是否 ok
 * 4. 测试 s 状态是否 Ok
 * 5. 调用 table builder finish 表明构建结束.
 * 6. file sync. 这里 sync 是有必要的, 因为如果这里不 sync, 那么当 BuildTable() 成功返回时, 数据可能仍然在内存
 * 中而未写入硬盘, 而且永远不确定有没有写入硬盘, 所以就有问题: 何时才能清除 log 文件呢?
 * 7. file close. 注意 file close 可能并不会 sync file
 * 8. 更新 filemeta, 并将 filemeta 写入 version edit.
 * 以上任何一个环节出错, 都导致 filename 被 delete.
 */
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  VersionEdit* edit) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  // 当 *iter 为空时, 不会创建 fname 指定的文件. 我觉得这里没有必要, 毕竟下面总会 DeleteFile
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      if (ExtractValueType(key) == kTypeLargeValueRef) {
        if (iter->value().size() != LargeValueRef::ByteSize()) {
          // 这么严格啊! 我觉得没有必要 check 的嘛. 另外 L0 啥意思? level 0?
          s = Status::Corruption("invalid indirect reference hash value (L0)");
          break;
        }
        edit->AddLargeValueRef(LargeValueRef::FromRef(iter->value()),
                               meta->number,
                               iter->key());
      }
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable.
      // 同时顺便把 meta->number 放在 cache 中, 根据程序局部性原理, 应该很快就会命中.
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {  // 如果 !s->ok(), 那么 s 自身状态不就丢失了没?
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    edit->AddFile(0, meta->number, meta->file_size,
                  meta->smallest, meta->largest);
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}
