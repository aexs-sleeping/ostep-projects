# Makefile for MapReduce Project

# 编译器和参数
CC = gcc

# C 标准：按你的要求默认使用 C11。
CSTD ?= c11

CFLAGS = -std=$(CSTD) -Wall -Werror -O2 -Iinclude -Isrc -pthread
LDFLAGS = -pthread

# 目录
SRC_DIR = src
INCLUDE_DIR = include
BUILD_DIR = build
TEST_DIR = test

# 源文件和目标文件
# 说明：csapp 是你自己的外部包装库，不作为 MapReduce 作业的必须部分。
# 为了避免环境/宏导致 csapp 编译失败，这里默认只构建 mapreduce。
MR_SRCS := $(SRC_DIR)/mapreduce.c
MR_OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(MR_SRCS))

# 头文件（对外暴露）
PUBLIC_HEADERS := $(wildcard $(INCLUDE_DIR)/*.h)

# 产物：静态库（该项目通常会被外部测试程序链接）
LIB = $(BUILD_DIR)/libmapreduce.a

# 测试程序
TESTS := $(wildcard $(TEST_DIR)/*.c)
TEST_BINS := $(patsubst $(TEST_DIR)/%.c,$(BUILD_DIR)/test_%, $(TESTS))

# 默认目标
all: $(LIB)

# 生成静态库（MapReduce）
$(LIB): $(MR_OBJS) | $(BUILD_DIR)
	ar rcs $@ $(MR_OBJS)

# (已移除) 可选 csapp 静态库目标 — 源文件已删除

# 编译目标文件
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c $(PUBLIC_HEADERS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# 编译测试程序
test: $(LIB) $(TEST_BINS)

$(BUILD_DIR)/test_%: $(TEST_DIR)/%.c $(LIB) $(PUBLIC_HEADERS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDFLAGS)

# 创建 build 目录
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# 清理
clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean test
