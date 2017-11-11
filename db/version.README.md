
Version 这块刚接触的时候很是迷糊. 所以这里先在高层面上按照个人理解总结一下 Version 及其相关设施的语义, 方便在看代码时心里有个底.


descriptor, 按我理解, descriptor 就是 server state, 存放着一个 leveldb 数据库的元信息, 如: 使用的 comparator 的 name, 下一个 table file 的 number 等. 完整的元信息项可以参考 VersionEdit 类的定义. descriptor 所有信息总是保存在内存中, 不过对 descriptor 的修改操作总会先持久化. 在打开一个 leveldb 时, 总会创建一个新 manifest 文件来存放最新的 descriptor.

descriptor 在内存中分散在多个地方存放, 如: Version 仅负责存放各个 level 的 file meta 信息, VersionSet 负责存储 next file number, compact pointers 等信息.

谁更改 descriptor 中的某一项就由谁负责在**合适的时候**持久化本次更改操作. 就像本来我以为的 `VersionSet::NewFileNumber()` 实现:

```cpp
int NewFileNumber() {
    next = next_file_number_ + 1;
    persist(next); // 生成个 VersionEdit 持久化 next;
    next_file_num_ ++;
    return next;
}
```

但是 VersionSet 并没有在 `NewFileNumber()` 中持久化 next file number, 而是在 LogAndApply() 中.


VersionEdit, 按我理解, 其用来表示对 server state 的变更, 也即存储引擎中经常说的 commit log. leveldb 的 VersionEdit 是可重入的, 不信你可以推一推.


manifest 文件, 其格式与 leveldb log file 一样, 其每一个 record 都是 VersionEdit 序列化后的结果.

本来按我设想会有两个文件来存储 descriptor: descriptor.snapshot, descriptor.log; 其中 descriptor.log 存储着对 descriptor 的变更; descriptor.snapshot 存储着 descriptor 某一时刻下的 snapshot. leveldb 会周期性的根据 descriptor.log 来重新生成 descriptor.snapshot, 同时清空 descriptor.log.

但是 leveldb 只用了一个 manifest 文件, 这个 manifest 最开始存放着 descriptor.snapshot, descriptor.log 会追加写到 manifest 末尾中. 而且在 manifest 中, snapshot, log 都是用 VersionEdit 来表示.


Version, leveldb 中的数据是分层存储的, 随着增删操作的实施, leveldb 各层数据不断变迁, Version 存储了某一时刻下各层所包含着的文件细节. The newest version is called "current".


VersionSet, 我觉得叫作 VersionManager 更为合适, The entire set of versions is maintained in a VersionSet, 即 VersionSet 负责管理一个 leveldb 数据库中所有 version, 其保存了不同时刻下的 Version 组成的链表, 负责新 Version 的创建初始化, 以及老 Version 的清除. 话说回来了, 代码里面的 VersionSet 可有点杂啊.



