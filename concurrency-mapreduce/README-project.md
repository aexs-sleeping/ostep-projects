# 项目实现说明与运行环境
**项目概览：**
- **目的**: 在单机环境下实现一个简化的 MapReduce 框架，支持用户自定义 `Map` 和 `Reduce` 函数，并保证线程安全与分区排序。
- **产物**: 静态库 `build/libmapreduce.a`，供测试程序或示例程序链接使用。

**实现要点：**
- **接口**: 遵循 `include/mapreduce.h` 中定义的接口：`MR_Emit`, `MR_DefaultHashPartition`, `MR_Run`。
- **中间数据**: 使用 `MR_KV` 结构存储复制后的 `(key,value)` 对，每个分区维护一块动态数组。
- **分区**: 用户可传入分区函数（`Partitioner`），默认使用 `MR_DefaultHashPartition`（djb2 哈希取模）。
- **排序**: 在 Map 阶段结束后，每个分区内按 `key` 的字典序升序排序，保证 Reduce 可以按键序遍历。

**主要数据结构（位于 `src/mapreduce.c`）**
- **`MR_KV`**: 存储单条中间 `(key,value)`，两者均由库复制并负责释放。
- **`MR_Partition`**: 每个分区包含动态数组 `items`、当前大小 `size`、容量 `cap`，以及用于 Reduce 迭代器的状态字段（`current_key` / 范围索引）。

**并发模型与线程实现**
- **Map 阶段**: 使用固定数量的 mapper 线程（由 `MR_Run` 的 `num_mappers` 指定）。这些线程共用一个任务索引 `next_index`；每个线程在一个 `while(1)` 循环中通过互斥读并递增索引来取下一个文件并调用用户 `Map(file)`。
	- `MR_Emit()` 是线程安全的：按分区对 `items` 动态数组的写入受该分区的互斥锁保护（`pthread_mutex_t lock`）。
- **Reduce 阶段**: 每个分区由单独的 reducer 线程独占处理；因此在归约时不需要额外锁。Reduce 通过库提供的 `Getter`（`MR_GetNext`）按键迭代当前 key 的所有 value。

**内存管理**
- `MR_Emit` 会复制 `key` 与 `value`（内部使用 `mr_strdup`）。
- 在作业结束后，框架会释放每个 `MR_KV` 的 `key`/`value` 字符串并释放分区数组，清理互斥锁。

**构建与运行**
- 依赖工具链：`gcc`（支持 `-std=c11`）、`make`、`pthread`（系统自带）。
- 在项目根目录执行：

```bash
make clean
make
```
- 产物位于：`build/libmapreduce.a`。要运行用户程序（例如单词计数示例），将示例程序与该静态库链接：

```bash
#gcc -std=c11 -Iinclude -Isrc -pthread example_wordcount.c build/libmapreduce.a -o wordcount
./wordcount file1.txt file2.txt
```


**注意事项与建议**
- 当前 Map 阶段采用共享索引调度，简单且易于理解，但在极高并发或任务粒度非常小的场景下可能造成负载不均。若未来需要更复杂的调度，建议引入任务队列或更精细的负载均衡策略。
- `MR_Emit` 里对内存分配失败的处理为直接 `exit(1)`；若要用于更健壮的生产环境，可改为返回错误码或采用更细粒度的错误恢复策略。

