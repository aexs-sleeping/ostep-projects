#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <stddef.h>

/**
 * @file threadpool.h
 * @brief 一个最小可用的线程池实现（内部模块）。
 *
 * 设计目标：
 * - 提供固定数量的工作线程（worker），通过任务队列处理提交的任务。
 * - submit() 只负责入队；destroy() 会等待所有任务完成后再回收线程。
 *
 * 说明：该线程池是 MapReduce 库的内部实现细节，不要求对外稳定 API。
 */

typedef struct threadpool threadpool_t;

typedef void (*threadpool_task_fn)(void *arg);

/**
 * @brief 创建线程池。
 * @param num_threads 工作线程数量，必须 > 0。
 * @return 成功返回线程池指针；失败返回 NULL。
 */
threadpool_t *threadpool_create(int num_threads);

/**
 * @brief 提交一个任务到线程池。
 *
 * 任务由某个工作线程异步执行：fn(arg)。
 *
 * @param pool 线程池。
 * @param fn 任务函数。
 * @param arg 任务参数。
 * @return 0 表示成功；-1 表示失败。
 */
int threadpool_submit(threadpool_t *pool, threadpool_task_fn fn, void *arg);

/**
 * @brief 销毁线程池。
 *
 * 语义：
 * - 等待所有已提交任务执行完毕。
 * - 然后停止所有工作线程并回收资源。
 *
 * @param pool 线程池。
 */
void threadpool_destroy(threadpool_t *pool);

#endif
