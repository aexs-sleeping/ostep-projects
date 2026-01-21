#include "mapreduce.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// =============================
//  内部数据结构与全局状态
// =============================

// 说明：
// - Map 阶段：多个 mapper 线程会并发调用 MR_Emit()
// - MR_Emit() 必须把 key/value 复制出来（不能直接保存用户传入指针）
// - Reduce 阶段：每个 partition 由一个 reducer 线程独占处理

typedef struct
{
    char *key;
    char *value;
} MR_KV;

// 每个分区的数据结构
typedef struct
{
    // Map 阶段累积的 (key,value) 列表
    MR_KV *items;
    size_t size;
    size_t cap;

    // 保护 items 的并发写入（仅 Map 阶段需要）
    pthread_mutex_t lock;
    // Reduce 阶段迭代器状态（一个 partition 只会被一个 reducer 线程访问）
    const char *current_key;
    size_t current_begin;
    size_t current_end;
    size_t current_pos; 
} MR_Partition;

static MR_Partition *g_partitions = NULL; // 分区数组
static int g_num_partitions = 0;          // 分区数量
static Partitioner g_partitioner = NULL;  // 分区函数
static Mapper g_mapper = NULL;            // 用户的 Map 函数
static Reducer g_reducer = NULL;          // 用户的 Reduce 函数

// =============================
//  工具函数（内部使用）
// =============================

/**
 * @brief 复制字符串到新分配的堆内存（内部使用）。
 *
 * 说明：C 标准库没有 strdup()；strdup() 属于 POSIX。
 * 为了避免不同环境下声明缺失/警告，把它实现成内部函数。
 *
 * @param s 输入字符串；允许为 NULL，视作空串 ""。
 * @return 新分配的字符串指针（调用者需 free）。
 */
static char *mr_strdup(const char *s)
{
    if (s == NULL)
    {
        s = "";
    }
    size_t n = strlen(s) + 1;
    char *p = (char *)malloc(n);
    if (p == NULL)
    {
        fprintf(stderr, "mr_strdup: out of memory\n");
        exit(1);
    }
    memcpy(p, s, n);
    return p;
}

/**
 * @brief 向分区追加一条 (key,value) 记录（内部使用）。
 *
 * 约束：
 * - 进入该函数前必须已持有 p->lock（因为会修改动态数组）。
 * - 函数内部会复制 key/value。
 *
 * @param p 分区指针。
 * @param key 键。
 * @param value 值。
 */
static void partition_append(MR_Partition *p, char *key, char *value)
{
    // 进入时应已持有 p->lock
    if (p->size == p->cap)
    {
        size_t new_cap = (p->cap == 0) ? 1024 : (p->cap * 2); // 扩容机制
        MR_KV *new_items = (MR_KV *)realloc(p->items, new_cap * sizeof(MR_KV));
        if (new_items == NULL)
        {
            fprintf(stderr, "partition_append: out of memory\n");
            exit(1);
        }
        p->items = new_items;
        p->cap = new_cap;
    }
    p->items[p->size].key = mr_strdup(key);
    p->items[p->size].value = mr_strdup(value);
    p->size++;
}

/**
 * @brief qsort 比较器：按 key 的字典序升序排序。
 * @param a 指向 MR_KV 的指针。
 * @param b 指向 MR_KV 的指针。
 * @return strcmp 风格返回值：<0 表示 a<b，0 表示相等，>0 表示 a>b。
 */
static int kv_key_cmp(const void *a, const void *b)
{
    const MR_KV *ka = (const MR_KV *)a;
    const MR_KV *kb = (const MR_KV *)b;
    return strcmp(ka->key, kb->key);
}

// =============================
//  Map 阶段：简单 worker 循环
// =============================

typedef struct
{
    char **files;
    int num_files;
    int next_index;
    pthread_mutex_t lock;
} MR_MapWork;

static void *mapper_thread(void *arg)
{
    MR_MapWork *work = (MR_MapWork *)arg;

    while (1)
    {
        char *file = NULL;

        pthread_mutex_lock(&work->lock);
        if (work->next_index < work->num_files)
        {
            file = work->files[work->next_index++];
        }
        pthread_mutex_unlock(&work->lock);

        if (file == NULL)
        {
            break;
        }

        g_mapper(file);
    }

    return NULL;
}

// Getter：Reduce() 通过它逐个拿到当前 key 的 value。
// 约定：
// - Reduce(key, get_next, partition) 被调用后，get_next 只能用于“同一个 key”
// - 当该 key 的 value 被取完后，返回 NULL
/**
 * @brief Reducer 迭代器：返回当前 key 对应的下一个 value。
 *
 * Reduce(key, get_next, partition) 被调用后，Reduce 会反复调用 get_next(key, partition)。
 * 当该 key 的 value 取完后返回 NULL。
 *
 * @param key 当前 Reduce() 正在处理的 key。
 * @param partition_number 分区号。
 * @return 指向 value 的指针；如果没有更多 value 则返回 NULL。
 */
static char *MR_GetNext(char *key, int partition_number)
{
    if (partition_number < 0 || partition_number >= g_num_partitions)
    {
        return NULL;
    }
    MR_Partition *p = &g_partitions[partition_number];
    if (p->current_key == NULL)
    {
        return NULL;
    }
    if (key == NULL || strcmp(key, p->current_key) != 0)
    {
        // 防御性检查：如果用户传错 key，直接返回 NULL。
        return NULL;
    }
    if (p->current_pos >= p->current_end)
    {
        return NULL;
    }
    return p->items[p->current_pos++].value;
}

/**
 * @brief 线程池任务：处理一个输入文件（调用用户 map(file)）。
 * @param arg 文件名（char*）。
 */
/**
 * @brief Reducer 线程工作函数：处理一个 partition（按 key 升序依次调用 reduce()）。
 *
 * 说明：
 * - 每个 partition 由一个 reducer 线程独占处理，因此 reducer 阶段不需要对分区加锁。
 * - 该函数会为每个 key 设置迭代器状态，使 MR_GetNext() 可以遍历该 key 的所有 value。
 *
 * @param arg 分区号（通过 (void*)(intptr_t) 传入）。
 * @return 线程返回值（始终为 NULL）。
 */
static void *reducer_thread(void *arg)
{
    int partition_number = (int)(intptr_t)arg;
    MR_Partition *p = &g_partitions[partition_number];

    // 分区内 items 已经按 key 排序。
    size_t i = 0;
    while (i < p->size)
    {
        const char *key = p->items[i].key;

        // 找到 [i, j) 范围内都是同一个 key
        size_t j = i + 1;
        while (j < p->size && strcmp(p->items[j].key, key) == 0)
        {
            j++;
        }

        // 设置当前 key 的迭代器状态
        p->current_key = key;
        p->current_begin = i;
        p->current_end = j;
        p->current_pos = i;

        // 调用用户 Reduce；它会通过 MR_GetNext() 取完该 key 的所有 value
        g_reducer((char *)key, MR_GetNext, partition_number);

        // 清空状态，避免误用
        p->current_key = NULL;
        p->current_begin = p->current_end = p->current_pos = 0;

        i = j;
    }
    return NULL;
}

// =============================
//  对外 API 实现
// =============================
/**
 * @brief 默认分区函数：将 key 映射到 0 ~ (num_partitions-1)。
 *
 * 使用 djb2 哈希。
 *
 * @param key 要分区的 key。
 * @param num_partitions 分区数量（通常等于 reducer 线程数）。
 * @return 分区号。
 */
unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
    {
        hash = hash * 33 + (unsigned long)c;
    }
    return (num_partitions <= 0) ? 0UL : (hash % (unsigned long)num_partitions);
}

/**
 * @brief 提交一个中间键值对 (key, value)。
 *
 * 由用户的 Map() 在 map 阶段调用。
 *
 * 关键要求：
 * - 必须复制 key/value（用户传入的字符串可能来自临时缓冲区，会被覆盖/释放）。
 * - 必须线程安全（多个 mapper 线程并发调用）。
 * - 必须依据 partitioner(key) 选择分区。
 *
 * @param key 键。
 * @param value 值。
 */
void MR_Emit(char *key, char *value)
{
    if (g_partitions == NULL || g_num_partitions <= 0 || g_partitioner == NULL)
    {
        // 没有初始化就调用，属于使用方式错误；直接忽略或终止都可以。
        // 为了更容易定位问题，这里选择终止。
        fprintf(stderr, "MR_Emit called before MR_Run initialization\n");
        exit(1);
    }

    unsigned long p = g_partitioner(key, g_num_partitions);
    int partition_number = (int)(p % (unsigned long)g_num_partitions);
    MR_Partition *part = &g_partitions[partition_number];

    pthread_mutex_lock(&part->lock);
    partition_append(part, key, value);
    pthread_mutex_unlock(&part->lock);
}

/**
 * @brief 运行一次 MapReduce 作业。
 *
 * 说明：
 * - 输入文件名来自 argv[1..argc-1]。
 * - 框架内部会创建 num_mappers 个 mapper 线程并发执行 map(file)。
 * - map 阶段结束后，对每个分区内的 (key,value) 按 key 升序排序。
 * - 创建 num_reducers 个 reducer 线程，每个线程处理一个分区，并按 key 顺序调用 reduce(key, get_next, partition)。
 * - 作业结束后释放 MR_Emit 中复制出的所有内存。
 *
 * @param argc 命令行参数个数。
 * @param argv 命令行参数数组，其中 argv[1..] 为输入文件名。
 * @param map 用户提供的 Map 函数。
 * @param num_mappers mapper 线程数量。
 * @param reduce 用户提供的 Reduce 函数。
 * @param num_reducers reducer 线程数量（同时也是分区数量）。
 * @param partition 分区函数；若为 NULL 则使用 MR_DefaultHashPartition。
 */

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
    if (argc < 2 || argv == NULL)
    {
        // 没有输入文件，直接返回即可。
        return;
    }

    if (map == NULL || reduce == NULL)
    {
        fprintf(stderr, "MR_Run: map/reduce must not be NULL\n");
        exit(1);
    }

    if (num_mappers <= 0 || num_reducers <= 0)
    {
        fprintf(stderr, "MR_Run: num_mappers/num_reducers must be > 0\n");
        exit(1);
    }

    // 默认 partitioner：如果用户传 NULL，就使用 MR_DefaultHashPartition
    if (partition == NULL)
    {
        partition = MR_DefaultHashPartition;
    }

    // 初始化全局状态（方便 MR_Emit / MR_GetNext 使用）
    g_mapper = map;
    g_reducer = reduce;
    g_partitioner = partition;
    g_num_partitions = num_reducers;    //桶的数量 等同于 reducer 数量

    // 输入文件
    int num_files = argc - 1;
    char **files = &argv[1];

    // 初始化 partitions
    g_partitions = (MR_Partition *)calloc((size_t)g_num_partitions, sizeof(MR_Partition));

    if (g_partitions == NULL)
    {
        fprintf(stderr, "MR_Run: out of memory\n");
        exit(1);
    }

    for (int i = 0; i < g_num_partitions; i++)
    {
        pthread_mutex_init(&g_partitions[i].lock, NULL);
        g_partitions[i].items = NULL;
        g_partitions[i].size = 0;
        g_partitions[i].cap = 0;
        g_partitions[i].current_key = NULL;
    }

    // 1) Map 阶段：固定 mapper 线程 + while(1) 循环取任务

    int mapper_workers = num_mappers;
    if (mapper_workers > num_files)
    {
        mapper_workers = num_files;
    }

    MR_MapWork work = {
        .files = files,
        .num_files = num_files,
        .next_index = 0,
    };
    pthread_mutex_init(&work.lock, NULL);

    pthread_t *mthreads = (pthread_t *)malloc((size_t)mapper_workers * sizeof(pthread_t));
    if (mthreads == NULL)
    {
        fprintf(stderr, "MR_Run: out of memory (mthreads)\n");
        exit(1);
    }

    for (int i = 0; i < mapper_workers; i++)
    {
        if (pthread_create(&mthreads[i], NULL, mapper_thread, &work) != 0)
        {
            fprintf(stderr, "MR_Run: pthread_create(mapper) failed\n");
            exit(1);
        }
    }
    for (int i = 0; i < mapper_workers; i++)
    {
        pthread_join(mthreads[i], NULL);
    }

    free(mthreads);
    pthread_mutex_destroy(&work.lock);

    //现在每个mapper线程获取完所有文件，并处理存储完毕
    // 2) Map 完成后，对每个 partition(分区) 排序
    for (int i = 0; i < g_num_partitions; i++)
    {
        MR_Partition *p = &g_partitions[i];
        // Map 已结束，此时不会再有并发写入；理论上不用加锁。
        if (p->size > 1)
        {
            qsort(p->items, p->size, sizeof(MR_KV), kv_key_cmp);
        }
    }

    // 3) 启动 reducer 线程：每个 partition 一个线程
    pthread_t *rthreads = (pthread_t *)malloc((size_t)num_reducers * sizeof(pthread_t));
    if (rthreads == NULL)
    {
        fprintf(stderr, "MR_Run: out of memory (rthreads)\n");
        exit(1);
    }
    for (int i = 0; i < num_reducers; i++)
    {
        if (pthread_create(&rthreads[i], NULL, reducer_thread, (void *)(intptr_t)i) != 0)
        {
            fprintf(stderr, "MR_Run: pthread_create(reducer) failed\n");
            exit(1);
        }
    }
    for (int i = 0; i < num_reducers; i++)
    {
        pthread_join(rthreads[i], NULL);
    }
    free(rthreads);

    // 4) 释放所有内部资源
    for (int i = 0; i < g_num_partitions; i++)
    {
        MR_Partition *p = &g_partitions[i];
        for (size_t k = 0; k < p->size; k++)
        {
            free(p->items[k].key);
            free(p->items[k].value);
        }
        free(p->items);
        pthread_mutex_destroy(&p->lock);
    }
    free(g_partitions);
    // 清空全局状态，避免用户误用（例如 MR_Run 结束后再调用 MR_Emit）
    g_partitions = NULL;
    g_num_partitions = 0;
    g_partitioner = NULL;
    g_mapper = NULL;
    g_reducer = NULL;
}