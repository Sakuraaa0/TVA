#!/usr/bin/env python3
"""
RocksDB MyBench 时序查询实验脚本 (num=32)
使用 RocksDB KV存储实现时序数据查询
"""

from rocksdict import Rdict, Options
import re
import time
import os
import random
import json

# 配置
NUM = "32"
DATASET_PATH = "/workspace/dataset/mybench"
RESULT_FILE = f"/workspace/CtGraph/experiment/rocksdb_mybench_result_{NUM}.txt"
DB_PATH = f"/workspace/CtGraph/experiment/rocksdb_mybench_{NUM}"
MAX_TS = 320000

def open_db():
    """打开RocksDB数据库"""
    opts = Options()
    opts.create_if_missing(True)
    return Rdict(DB_PATH, opts)

def clear_db():
    """清除旧数据库"""
    import shutil
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)

# Key设计:
# user:{id}:ts:{timestamp} -> JSON(用户数据)
# friend:{src}:{dst}:ts:{timestamp} -> "1"(添加) 或 "0"(删除)

def user_key(uid, ts):
    return f"user:{uid:08d}:ts:{ts:010d}"

def friend_key(src, dst, ts):
    return f"friend:{src:08d}:{dst:08d}:ts:{ts:010d}"

def load_initial_data(db):
    """加载初始数据"""
    with open(f"{DATASET_PATH}/small.cypher", "r") as f:
        for line in f:
            line = line.strip()
            if "CREATE (:User" in line:
                m = re.search(r'id:\s*(\d+).*completion_percentage:\s*(\d+).*gender:\s*"(\w+)".*age:\s*(\d+)', line)
                if m:
                    uid, cp, gender, age = m.groups()
                    uid = int(uid)
                    data = {"id": uid, "cp": int(cp), "gender": 1 if gender == "man" else 0, "age": int(age)}
                    db[user_key(uid, 0)] = json.dumps(data)
            elif "MATCH" in line and "Friend" in line:
                ids = re.findall(r'id:\s*(\d+)', line)
                if len(ids) >= 2:
                    src, dst = int(ids[0]), int(ids[1])
                    db[friend_key(src, dst, 0)] = "1"
                    db[friend_key(dst, src, 0)] = "1"

def load_operations(db):
    """加载操作数据"""
    with open(f"{DATASET_PATH}/{NUM}/cypher.txt", "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            ts = line_num

            if "SET n.completion_percentage" in line and "CREATE" not in line:
                m = re.search(r'id:(\d+).*=\s*(\d+)', line)
                if m:
                    uid, cp = int(m.group(1)), int(m.group(2))
                    data = get_user_at_time_internal(db, uid, ts - 1)
                    if not data:
                        continue
                    data["cp"] = cp
                    db[user_key(uid, ts)] = json.dumps(data)

            elif "CREATE (n:User" in line:
                m = re.search(r'id\s*:\s*(\d+).*=\s*(\d+)', line)
                if m:
                    uid, cp = int(m.group(1)), int(m.group(2))
                    data = {"id": uid, "cp": cp, "gender": 2, "age": 0}
                    db[user_key(uid, ts)] = json.dumps(data)

            elif "CREATE (a)-[:Friend]->(b)" in line:
                ids = re.findall(r'id:\s*(\d+)', line)
                if len(ids) >= 2:
                    src, dst = int(ids[0]), int(ids[1])
                    db[friend_key(src, dst, ts)] = "1"
                    db[friend_key(dst, src, ts)] = "1"

            elif "DELETE e" in line:
                ids = re.findall(r'id:(\d+)', line)
                if len(ids) >= 2:
                    src, dst = int(ids[0]), int(ids[1])
                    db[friend_key(src, dst, ts)] = "0"
                    db[friend_key(dst, src, ts)] = "0"

            if line_num % 10000 == 0:
                print(f"已处理 {line_num} 行操作")

def get_user_at_time_internal(db, uid, ts):
    """内部函数：获取指定时间戳的用户数据"""
    prefix = f"user:{uid:08d}:ts:"
    target = user_key(uid, ts)

    # 使用迭代器找到 <= ts 的最大key
    it = db.iter()
    it.seek(target)

    # 检查当前位置
    if it.valid():
        key = it.key()
        if key.startswith(prefix):
            return json.loads(it.value())
        # 当前key大于target，需要向前
        it.prev()

    if it.valid():
        key = it.key()
        if key.startswith(prefix):
            return json.loads(it.value())

    return None

def get_user_at_time(db, uid, ts):
    """获取指定时间戳的用户数据 (Q1)"""
    return get_user_at_time_internal(db, uid, ts)

def get_user_in_range(db, uid, from_ts, to_ts):
    """获取时间范围内的用户数据 (Q2)"""
    results = []
    prefix = f"user:{uid:08d}:ts:"
    start_key = user_key(uid, from_ts)
    end_key = user_key(uid, to_ts)

    it = db.iter()
    it.seek(start_key)

    while it.valid():
        key = it.key()
        if not key.startswith(prefix) or key > end_key:
            break
        results.append(json.loads(it.value()))
        it.next()

    return results

def get_friends_at_time(db, uid, ts):
    """获取指定时间戳的好友列表 (Q3)"""
    friends = set()
    prefix = f"friend:{uid:08d}:"

    it = db.iter()
    it.seek(prefix)

    while it.valid():
        key = it.key()
        if not key.startswith(prefix):
            break

        # 解析 friend:{src}:{dst}:ts:{timestamp}
        parts = key.split(":")
        if len(parts) >= 5:
            dst = int(parts[2])
            key_ts = int(parts[4])
            if key_ts <= ts:
                val = it.value()
                if val == "1":
                    friends.add(dst)
                elif val == "0":
                    friends.discard(dst)
        it.next()

    return list(friends)

def run_q1(db, result_file):
    """执行 Q1 查询"""
    result_file.write("\n开始执行Q1查询...\n")
    start = time.time()

    with open(f"{DATASET_PATH}/{NUM}/cypher_Q1.txt", "r") as f:
        for line in f:
            m = re.search(r'id\s*:\s*(\d+)', line)
            if m:
                uid = int(m.group(1))
                ts = random.randint(1, MAX_TS)
                get_user_at_time(db, uid, ts)

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q1查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q2(db, result_file):
    """执行 Q2 查询"""
    result_file.write("\n开始执行Q2查询...\n")
    start = time.time()

    with open(f"{DATASET_PATH}/{NUM}/cypher_Q2.txt", "r") as f:
        for line in f:
            m = re.search(r'id\s*:\s*(\d+)', line)
            if m:
                uid = int(m.group(1))
                from_ts = random.randint(1, MAX_TS - 100)
                to_ts = from_ts + 100
                # 对范围内每个时间点分别查询
                for ts in range(from_ts, to_ts + 1):
                    get_user_at_time(db, uid, ts)

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q2查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q3(db, result_file):
    """执行 Q3 查询"""
    result_file.write("\n开始执行Q3查询...\n")
    start = time.time()

    with open(f"{DATASET_PATH}/{NUM}/cypher_Q3.txt", "r") as f:
        for line in f:
            m = re.search(r'id\s*:\s*(\d+)', line)
            if m:
                uid = int(m.group(1))
                ts = random.randint(1, MAX_TS)
                get_user_at_time(db, uid, ts)
                friends = get_friends_at_time(db, uid, ts)
                for fid in friends:
                    get_user_at_time(db, fid, ts)

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q3查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q4(db, result_file):
    """执行 Q4 查询"""
    result_file.write("\n开始执行Q4查询...\n")
    start = time.time()

    with open(f"{DATASET_PATH}/{NUM}/cypher_Q4.txt", "r") as f:
        for line in f:
            m = re.search(r'id\s*:\s*(\d+)', line)
            if m:
                uid = int(m.group(1))
                from_ts = random.randint(1, MAX_TS - 100)
                to_ts = from_ts + 100
                # 对范围内每个时间点分别查询
                for ts in range(from_ts, to_ts + 1):
                    get_user_at_time(db, uid, ts)
                    friends = get_friends_at_time(db, uid, ts)
                    for fid in friends:
                        get_user_at_time(db, fid, ts)

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q4查询执行时间: {int(duration)} 微秒\n")
    return duration

def get_db_size():
    """获取数据库大小"""
    total = 0
    for root, dirs, files in os.walk(DB_PATH):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total

def main():
    print("清除旧数据库...")
    clear_db()

    db = open_db()

    with open(RESULT_FILE, "w") as result_file:
        print("加载初始数据...")
        start_init = time.time()
        load_initial_data(db)
        init_duration = (time.time() - start_init) * 1000
        init_size = get_db_size()

        print("加载操作数据...")
        start_ops = time.time()
        load_operations(db)
        ops_duration = (time.time() - start_ops) * 1000
        total_size = get_db_size()

        total_load = init_duration + ops_duration
        result_file.write(f"初始数据加载时间: {int(init_duration)} 毫秒\n")
        result_file.write(f"操作数据加载时间: {int(ops_duration)} 毫秒\n")
        result_file.write(f"总加载时间: {int(total_load)} 毫秒\n")

        result_file.write(f"\n存储占用统计:\n")
        result_file.write(f"初始数据大小: {init_size/1024/1024:.2f} MB\n")
        result_file.write(f"时序数据大小: {(total_size-init_size)/1024/1024:.2f} MB\n")
        result_file.write(f"RocksDB总大小: {total_size/1024/1024:.2f} MB\n")

        d1 = run_q1(db, result_file)
        d2 = run_q2(db, result_file)
        d3 = run_q3(db, result_file)
        d4 = run_q4(db, result_file)

        total = d1 + d2 + d3 + d4
        result_file.write(f"\n总执行时间: {int(total)} 微秒\n")

    db.close()
    print(f"结果已写入 {RESULT_FILE}")

if __name__ == "__main__":
    main()
