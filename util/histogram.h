// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include <string>

namespace leveldb {

/* Histogram 用来对一堆值进行一些统计操作, 比如: 最小值, 最大值, 平均值, 方差等.
 *
 * 以后在进行请求时间, 某个操作的处理时间统计时可以采用 Histogram 的做法.
 */
class Histogram {
 public:
  Histogram() { }  // 构造函数里面不得调用一次 Clear() 么
  ~Histogram() { }

  // 清除之前 Add() 的信息, 使得当前 Histogram 状态变为未被 Add() 过一样.
  void Clear();
  void Add(double value);

  std::string ToString() const;

 private:
  double min_;  // 最小值.
  double max_;  // 最小值.
  double num_;  // 待统计值的个数.
  double sum_;  // 待统计值之和.
  double sum_squares_;  // 待统计值平方的和. 所以不应该叫 squares_sum_ 么?==

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];
  // buckets_[i] 存放着取值介于 [kBucketLimit[i - 1], kBucketLimit[i]) 之间的值的个数.
  double buckets_[kNumBuckets];

  double Median() const;
  // 计算 p% 的值的边界. 如: Percentile(90.0) == 1.3 表明 90% 的值都小于 1.3
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;  // 标准差, 参见 wiki
};

}

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
