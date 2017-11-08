// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "include/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table, and
// large value refs and the added file information will be added to
// *edit.  If no data is present in *iter, meta->file_size will be set
// to zero, and no Table file will be produced.
// 这个时候才会把 large value refs 更新到 VersionEdit 中啊. 不过确实只有这个时候才能更新, 只有这个时候才知道
// file number 嘛.
//
// 当返回 status 不是 ok 时, edit 应被清空.
extern Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         TableCache* table_cache,
                         Iterator* iter,
                         FileMetaData* meta,
                         VersionEdit* edit);

}

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
