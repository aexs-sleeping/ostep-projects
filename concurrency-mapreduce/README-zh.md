# Map Reduce（映射归约）

2004年，Google 工程师提出了一种用于大规模并行数据处理的新范式——MapReduce（可参考原始论文[这里](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)，并注意查阅文末的引用文献）。MapReduce 的一个关键特点是让开发者能够轻松地在大规模集群上编程，无需关心并行管理、机器故障处理等复杂问题，开发者只需专注于编写少量代码（如下所述），其余由基础设施自动处理。

本项目将让你在单机上实现一个简化版的 MapReduce。虽然在单机上实现 MapReduce 相对容易，但在实现正确的并发支持时仍有诸多挑战。因此，你需要思考如何设计 MapReduce，并高效、正确地实现它。

本作业有三个具体目标：

- 了解 MapReduce 范式的基本原理。
- 使用线程及相关函数实现一个正确且高效的 MapReduce 框架。
- 获得更多编写并发代码的经验。

## 背景

要顺利完成任何涉及并发的项目，你应了解线程创建、互斥锁（lock）以及条件变量（condition variable）的基本知识。推荐阅读以下章节：

- [线程简介](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-intro.pdf)
- [线程 API](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-api.pdf)
- [锁](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-locks.pdf)
- [锁的使用](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-locks-usage.pdf)
- [条件变量](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-cv.pdf)

请认真阅读这些内容，为本项目做好准备。

## 总体思路

你需要实现的 MapReduce 框架支持用户自定义的 `Map()` 和 `Reduce()` 函数。

原论文描述：“用户编写的 `Map()` 接收一个输入对，生成一组中间键值对。MapReduce 库会将所有具有相同中间键 K 的值聚合在一起，并传递给 `Reduce()` 函数。”

“同样由用户编写的 `Reduce()` 函数，接收一个中间键 K 及其对应的一组值。它将这些值合并，通常每次 `Reduce()` 调用只输出零个或一个值。中间值通过迭代器传递给用户的 reduce 函数。”

一个经典例子（伪代码）如下，用于统计一组文档中每个单词出现的次数：

```
map(String key, String value):
    // key: 文档名
    // value: 文档内容
    for each word w in value:
        EmitIntermediate(w, "1");

reduce(String key, Iterator values):
    // key: 单词
    // values: 计数字符串列表
    int result = 0;
    for each v in values:
        result += ParseInt(v);
    print key, result;
```

MapReduce 的魅力在于，许多不同类型的计算都可以映射到这个框架。原论文列举了许多例子，包括单词计数（如上）、分布式 grep、URL 访问频率统计、反向网页链接图、每主机的词向量分析等。

MapReduce 还非常易于并行化：可以同时运行多个 mapper，随后也可以同时运行多个 reducer。用户无需关心如何并行化，只需编写 `Map()` 和 `Reduce()`，其余由框架完成。

## 代码概览

我们为你提供了 [`mapreduce.h`](https://github.com/remzi-arpacidusseau/ostep-projects/tree/master/concurrency-mapreduce/mapreduce.h) 头文件，规定了你需要实现的 MapReduce 库接口：

```
#ifndef __mapreduce_h__
#define __mapreduce_h__

// MR 用到的不同函数指针类型
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// 你需要实现的外部函数
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition);

#endif // __mapreduce_h__
```

最重要的函数是 `MR_Run`，它接收命令行参数、Map 函数指针（类型为 `Mapper`，名为 `map`）、要创建的 mapper 线程数（`num_mappers`）、Reduce 函数指针（类型为 `Reducer`，名为 `reduce`）、reducer 线程数（`num_reducers`），以及分区函数指针（`partition`，见下文）。

因此，用户在用你的库编写 MapReduce 程序时，需要实现 Map、Reduce、（可选）Partition 函数，并调用 `MR_Run()`。框架会自动创建线程并运行。

一个基本假设是，库会创建 `num_mappers` 个线程（线程池），每个线程处理一个 Map 任务。还会创建 `num_reducers` 个线程处理 Reduce 任务。你的库还需设计内部数据结构，将 mappers 产生的键值对传递给 reducers，详见下文。

## 简单示例：单词计数

下面是一个使用该框架的简单单词计数程序：

```
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
```

代码说明：
- `Map()` 接收文件名，读取文件内容，将每个单词作为 key，1 作为 value，调用 `MR_Emit()`。
- `MR_Emit()` 由你的库实现，负责存储所有 key/value 对，供后续 Reduce 阶段使用。
- mappers 完成后，库应将 key/value 对存储好，`Reduce()` 会被调用。`Reduce()` 每次处理一个 key，通过 `get_next()` 迭代所有 value，统计出现次数并输出。
- 所有计算由 `main()` 中的 `MR_Run()` 启动。`argv[1]` 到 `argv[n-1]` 是要处理的文件名。
- 分区函数 `MR_DefaultHashPartition` 用于决定 key 属于哪个分区（即哪个 reducer 线程）。

`MR_DefaultHashPartition` 实现如下：

```
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
```

该函数将 key 映射到 0 ~ num_partitions-1 之间的分区号。你的 MR 库应使用该函数决定每个 key/value 对属于哪个分区（即哪个 reducer 线程）。

最后一个要求：每个分区内，key 及其 value 列表需按 key 升序排序。即 reducer 线程处理时，需按序遍历每个 key。

## 实现要点

- **线程管理**：需创建 `num_mappers` 个 mapper 线程，为每个 Map 分配文件（可用轮询、最短文件优先等策略）。还需创建 `num_reducers` 个 reducer 线程。
- **分区与排序**：核心数据结构需支持并发，允许 mappers 正确高效地将数据放入不同分区。mappers 完成后，需对每个分区的 key/value 列表排序。reducer 线程需按序处理每个 key。注意锁的使用，保证正确性。
- **内存管理**：`MR_Emit()` 传入的 key/value 需由 MR 库复制。所有映射和归约结束后，MR 库负责释放所有内存。

## 评分标准

你的代码应提交 `mapreduce.c`，正确高效地实现上述功能。编译时会加上 `-Wall -Werror -pthread -O`，并用 valgrind 检查内存错误。
首先会测试正确性，确保 Map 和 Reduce 正确执行。通过后会测试性能，性能越高得分越高。
