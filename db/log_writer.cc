// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "include/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record.
  // 注意这里是讲当 slice 为空, 也即用户传入的 slice 为空时才 emit a single zero-length record! 我最开始
  // 以为是当 left 为 0 时也要再循环一次 emit a zero-length record 呢!
  Status s;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover <= kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave <= kHeaderSize bytes in a block.
    // 这个与 doc/log_format.txt 里面讲的可不一样啊, 如果按照 doc/log_format.txt 的说法这里应该是:
    //  const int avail = kBlockSize - block_offset_;
    // 不过虽然有一点差异, 但仍然是兼容的, 所以不影响 log reader.
    // Q: 哪为啥要 leave kHeaderSize 呢?
    const int avail = kBlockSize - block_offset_ - kHeaderSize;
    assert(avail > 0);

    const size_t fragment_length = (left < avail) ? left : avail;

    // 这段挺机智的.
    RecordType type;
    const bool begin = (ptr == slice.data());
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    // Q: EmitPhysicalRecord() 失败会导致 AddRecord() 提前返回, 那么后续还会调用 AddRecord() 么? 如果
    // 继续调用, 那么后续 AddRecord() 可能就被污染了哇!
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  // 按我理解 EmitPhysicalRecord() 在失败时应该保持原样吧, 也即不应该更新 block_offset_ 吧当失败时.
  block_offset_ += kHeaderSize + n;
  return s;
}

}
}
