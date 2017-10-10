// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
#define STORAGE_LEVELDB_INCLUDE_OPTIONS_H_

#include <stddef.h>

namespace leveldb {

class Cache;
class Comparator;
class Env;
class Snapshot;
class WritableFile;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression           = 0x0,
  kLightweightCompression  = 0x1,
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator;

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.  This may have unforeseen ramifications: for example, a
  // corruption of one DB entry may cause a large number of entries to
  // become unreadable or for the entire DB to become unopenable.
  // Default: false
  bool paranoid_checks;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // Any internal progress/error information generated by the db will
  // be to written to info_log if it is non-NULL, or to a file stored
  // in the same directory as the DB contents if info_log is NULL.
  // Default: NULL
  WritableFile* info_log;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory before converting to an
  // on-disk file.
  //
  // Some DB operations may encounter a delay proportional to the size
  // of this parameter.  Therefore we recommend against increasing
  // this parameter unless you are willing to live with an occasional
  // slow operation in exchange for faster bulk loading throughput.
  //
  // Default: 1MB
  size_t write_buffer_size;

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set (budget
  // one open file per 2MB of working set).
  //
  // Default: 1000
  int max_open_files;

  // Handle values larger than "large_value_threshold" bytes
  // specially, by writing them into their own files (to avoid
  // compaction overhead) and doing content-based elimination of
  // duplicate values to save space.
  //
  // We recommend against changing this value.
  //
  // Default: 64K
  size_t large_value_threshold;

  // Control over blocks (user data is stored in a set of blocks, and
  // a block is the unit of reading from disk).

  // Use the specified cache for blocks (if non-NULL).
  // Default: NULL
  Cache* block_cache;

  // Approximate size of user data packed per block.  Note that the
  // block size specified here corresponds to uncompressed data.  The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled.  This parameter can be changed dynamically.
  //
  // Default: 8K
  int block_size;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.
  //
  // Default: 16
  //
  // block_restart_interval 的语义参见 block_builder.cc; 我一开始理解错了.
  int block_restart_interval;

  // Compress blocks using the specified compression algorithm.  This
  // parameter can be changed dynamically.
  //
  // Default: kLightweightCompression, which gives lightweight but fast
  // compression.
  //
  // Typical speeds of kLightweightCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kLightweightCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  CompressionType compression;

  // Create an Options object with default values for all fields.
  Options();
};

// Options that control read operations
struct ReadOptions {
  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: false
  bool verify_checksums;

  // Should the data read for this iteration be cached in memory?
  // Callers may wish to set this field to false for bulk scans.
  // Default: true
  bool fill_cache;

  // If "snapshot" is non-NULL, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is NULL, use an implicit
  // snapshot of the state at the beginning of this read operation.
  // Default: NULL
  const Snapshot* snapshot;

  ReadOptions()
      : verify_checksums(false),
        fill_cache(true),
        snapshot(NULL) {
  }
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // Default: true
  bool sync;

  // If "post_write_snapshot" is non-NULL, and the write succeeds,
  // *post_write_snapshot will be modified to point to a snapshot of
  // the DB state immediately after this write.  The caller must call
  // DB::ReleaseSnapshot(*post_write_snapshot) when the
  // snapshot is no longer needed.
  //
  // If "post_write_snapshot" is non-NULL, and the write fails,
  // *post_write_snapshot will be set to NULL.
  //
  // Default: NULL
  const Snapshot** post_write_snapshot;

  WriteOptions()
      : sync(true),
        post_write_snapshot(NULL) {
  }
};

}

#endif  // STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
