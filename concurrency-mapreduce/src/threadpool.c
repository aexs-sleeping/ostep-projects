#include "threadpool.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

/**
 * @brief 线程池任务节点。
 */
typedef struct tp_task {
    threadpool_task_fn fn;
    void *arg;
    struct tp_task *next;
} tp_task_t;

struct threadpool {
    pthread_t *threads;
    int num_threads;

    tp_task_t *head;
    tp_task_t *tail;

    // pending：已提交但尚未完成的任务数量
    size_t pending;

    int stop;

    pthread_mutex_t lock;
    pthread_cond_t has_task;
    pthread_cond_t empty;
};

/**
 * @brief 工作线程主循环。
 */
static void *tp_worker(void *arg) {
    threadpool_t *pool = (threadpool_t *)arg;

    while (1) {
        tp_task_t *task = NULL;

        pthread_mutex_lock(&pool->lock);
        while (!pool->stop && pool->head == NULL) {
            pthread_cond_wait(&pool->has_task, &pool->lock);
        }

        if (pool->stop && pool->head == NULL) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }

        task = pool->head;
        pool->head = task->next;
        if (pool->head == NULL) {
            pool->tail = NULL;
        }
        pthread_mutex_unlock(&pool->lock);

        // 执行任务（不持锁）
        task->fn(task->arg);
        free(task);

        pthread_mutex_lock(&pool->lock);
        pool->pending--;
        if (pool->pending == 0 && pool->head == NULL) {
            pthread_cond_broadcast(&pool->empty);
        }
        pthread_mutex_unlock(&pool->lock);
    }

    return NULL;
}

threadpool_t *threadpool_create(int num_threads) {
    if (num_threads <= 0) {
        return NULL;
    }

    threadpool_t *pool = (threadpool_t *)calloc(1, sizeof(threadpool_t));
    if (pool == NULL) {
        return NULL;
    }

    pool->num_threads = num_threads;
    pool->threads = (pthread_t *)calloc((size_t)num_threads, sizeof(pthread_t));
    if (pool->threads == NULL) {
        free(pool);
        return NULL;
    }

    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->has_task, NULL);
    pthread_cond_init(&pool->empty, NULL);

    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&pool->threads[i], NULL, tp_worker, pool) != 0) {
            // 创建失败：设置 stop 并回收已创建线程
            pthread_mutex_lock(&pool->lock);
            pool->stop = 1;
            pthread_cond_broadcast(&pool->has_task);
            pthread_mutex_unlock(&pool->lock);

            for (int j = 0; j < i; j++) {
                pthread_join(pool->threads[j], NULL);
            }
            pthread_cond_destroy(&pool->empty);
            pthread_cond_destroy(&pool->has_task);
            pthread_mutex_destroy(&pool->lock);
            free(pool->threads);
            free(pool);
            return NULL;
        }
    }

    return pool;
}

int threadpool_submit(threadpool_t *pool, threadpool_task_fn fn, void *arg) {
    if (pool == NULL || fn == NULL) {
        return -1;
    }

    tp_task_t *task = (tp_task_t *)calloc(1, sizeof(tp_task_t));
    if (task == NULL) {
        return -1;
    }
    task->fn = fn;
    task->arg = arg;

    pthread_mutex_lock(&pool->lock);
    if (pool->stop) {
        pthread_mutex_unlock(&pool->lock);
        free(task);
        return -1;
    }

    //增加或者减少的逻辑
    if (pool->tail == NULL) {
        pool->head = pool->tail = task;
    } else {
        pool->tail->next = task;
        pool->tail = task;
    }

    pool->pending++;
    pthread_cond_signal(&pool->has_task);
    pthread_mutex_unlock(&pool->lock);

    return 0;
}

void threadpool_destroy(threadpool_t *pool) {
    if (pool == NULL) {
        return;
    }

    // 等待任务全部完成
    pthread_mutex_lock(&pool->lock);
    while (pool->pending != 0 || pool->head != NULL) {
        pthread_cond_wait(&pool->empty, &pool->lock); 
        //等待empty信号
    }

    // 停止 worker
    pool->stop = 1;
    pthread_cond_broadcast(&pool->has_task);
    pthread_mutex_unlock(&pool->lock);

    for (int i = 0; i < pool->num_threads; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_cond_destroy(&pool->empty);
    pthread_cond_destroy(&pool->has_task);
    pthread_mutex_destroy(&pool->lock);

    free(pool->threads);
    free(pool);
}
