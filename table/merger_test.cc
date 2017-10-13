#include <vector>
#include <string>
#include <algorithm>

#include "table/merger.h"

#include "include/comparator.h"
#include "include/iterator.h"
#include "include/slice.h"
#include "include/status.h"

#include "util/testharness.h"

int Slice2int(const leveldb::Slice &target) {
    return std::stoi(target.ToString());
}


class TestComparator: public leveldb::Comparator {
public:
    int Compare(const Slice& a, const Slice& b) const override {
        return Slice2int(a) - Slice2int(b);
    }

    const char* Name() const override {
        return "0x66ccff";
    }

    void FindShortestSeparator(std::string*, const Slice&) const override {
    }

    void FindShortSuccessor(std::string*) const override {
    }
};


class TestIterator: public leveldb::Iterator {
public:
    TestIterator(const std::vector<int> &source):
        data_(source),
        current_idx_(0) {
    }

    bool Valid() const override {
        return current_idx_ >= 0 && current_idx_ < data_.size();
    }

    void SeekToFirst() override {
        current_idx_ = 0;
        return ;
    }

    void SeekToLast() override {
        current_idx_ = data_.size() - 1;
        return ;
    }

    void Seek(const leveldb::Slice& target) override {
        int t = Slice2int(target);
        // c++ 标准貌似没有规定 std::vector::iterator 差值等于 idx 哈.
        return std::lower_bound(data_.cbegin(), data_.cend(), t) - data_.cbegin();
    }

    void Next() override {
        ++current_idx_;
        return ;
    }

    void Prev() override {
        --current_idx_;
        return ;
    }

    leveldb::Slice key() const override {
        key_ = std::to_string(data_[current_idx_]);
        return leveldb::Slice(key_);
    }

    leveldb::Slice value() const override {
        return key();
    }

    leveldb::Status status() const override {
        return leveldb::Status::OK();
    }

private:
    std::vector<int> data_;
    int current_idx_;

    std::string key_;
};


namespace leveldb {

TEST(Merger, Test) {
    TestIterator iter1({1, 3, 4});
    TestIterator iter2({2, 4, 6});
    std::vector<TestIterator*> iters{&iter1, &iter2};

    TestComparator comp;
    Iterator *merger_iter = NewMergingIterator(&comp, iters.data(), iters.size());

    merger_iter->SeekToFirst();
    ASSERT_EQ(1, merger_iter->key());
}

}


int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
