// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {

  // 要写入的实际数据
  const char* ptr = slice.data();
  // 要写入数据的剩余长度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;

  // 是否是记录的第一个片段
  bool begin = true;
  do {
    // 计算当前块中剩余的可用空间，用块的大小减去当前已写入的偏移量
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);

    // 如果剩余的空间不足以容纳记录头部，则填充剩余空间并开始一个新块
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        // 用零字节填充剩余空间，确保块对齐
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 重新开始一个新块，重置块偏移量

      // 注意，这里并没有实际创建一个新的块对象，而是通过重置偏移量来模拟新块的开始
      // 也就是多个Block都是连续写入到同一个WritableFile中，只是通过block_offset_来区分块的边界
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 计算当前块中可用来存放记录数据的空间，用块大小减去当前偏移量和头部大小，得到的就是剩余的可用空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 计算当前片段的长度。如果还未写入的数据小于可用大小，则当前片段长度为剩余数据长度，否则为可用大小
    // 保证写入的片段不会超过当前块的可用空间
    const size_t fragment_length = (left < avail) ? left : avail;

    // 判断当前片段的类型：完整片段、首片段、中间片段或尾片段
    RecordType type;
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
    // 将当前片段作为物理记录写入日志
    s = EmitPhysicalRecord(type, ptr, fragment_length);

    // 移动指针和更新剩余长度，准备写入下一个片段
    ptr += fragment_length;
    left -= fragment_length;

    // 更新状态，标记已经开始写入，不是首片段了
    begin = false;

    // 如果写入成功且还有剩余数据，继续循环写入下一个片段
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  // 断言，确保写入的长度不会超过两个字节的表示范围，
  // 因为Log的头部表示长度字段只有两个字节
  assert(length <= 0xffff);  // Must fit in two bytes

  // 断言，确保当前块的偏移量加上头部大小和数据长度不会超过块的总大小
  // 这保证了每个block都是刚好的kBlockSize，不会溢出
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];

  // 第五和第六个字节，用小端序存储长度信息
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);

  // 第七个字节，存储记录类型
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.

  // 计算记录类型和负载的CRC校验值
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage

  // 将crc写入头四个字节，使用小端序存储
  EncodeFixed32(buf, crc);

  // Write the header and the payload

  // 写入头部
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    // 写入数据负载
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      // 刷新数据到存储设备，确保数据持久化
      s = dest_->Flush();
    }
  }
  // 更新当前块的偏移量，增加头部和数据长度
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
