#!/usr/bin/env python3
"""
PostgreSQL MyBench 时序查询实验脚本 (num=32)
将图数据库的 Cypher 查询转换为 PostgreSQL 时序查询
"""

import psycopg2
import re
import time
import os
import random

# 配置
NUM = "32"
DATASET_PATH = "/workspace/dataset/mybench"
RESULT_FILE = f"/workspace/CtGraph/experiment/pg_mybench_result_{NUM}.txt"
MAX_TS = 320000  # 操作数据的最大行号/时间戳

def get_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        database="mybench",
        user="postgres",
        host="/var/run/postgresql"
    )

def setup_schema(conn):
    """初始化表结构"""
    with open("/workspace/CtGraph/experiment/pg_mybench_schema.sql", "r") as f:
        schema_sql = f.read()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()

def load_initial_data(conn):
    """加载初始数据 (small.cypher)"""
    with conn.cursor() as cur:
        # 设置初始时间为0
        cur.execute("SELECT set_system_time(line_to_ts(0))")
        with open(f"{DATASET_PATH}/small.cypher", "r") as f:
            for line in f:
                line = line.strip()
                if "CREATE (:User" in line:
                    m = re.search(r'id:\s*(\d+).*completion_percentage:\s*(\d+).*gender:\s*"(\w+)".*age:\s*(\d+)', line)
                    if m:
                        uid, cp, gender, age = m.groups()
                        gender_val = 1 if gender == "man" else 0
                        cur.execute(
                            "INSERT INTO users (id, completion_percentage, gender, age) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (int(uid), int(cp), gender_val, int(age))
                        )
                elif "MATCH" in line and "Friend" in line:
                    ids = re.findall(r'id:\s*(\d+)', line)
                    if len(ids) >= 2:
                        src, dst = int(ids[0]), int(ids[1])
                        cur.execute(
                            "INSERT INTO friends (src_id, dst_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (src, dst)
                        )
                        cur.execute(
                            "INSERT INTO friends (src_id, dst_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (dst, src)
                        )
    conn.commit()

def load_operations(conn):
    """加载操作数据 (cypher.txt) - 模拟时序更新"""
    with conn.cursor() as cur:
        with open(f"{DATASET_PATH}/{NUM}/cypher.txt", "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                # 设置当前时间戳（每次操作前设置）
                cur.execute("SELECT set_system_time(line_to_ts(%s))", (line_num,))

                if "SET n.completion_percentage" in line and "CREATE" not in line:
                    m = re.search(r'id:(\d+).*=\s*(\d+)', line)
                    if m:
                        uid, cp = int(m.group(1)), int(m.group(2))
                        cur.execute(
                            "UPDATE users SET completion_percentage = %s WHERE id = %s",
                            (cp, uid)
                        )
                elif "CREATE (n:User" in line:
                    m = re.search(r'id\s*:\s*(\d+).*=\s*(\d+)', line)
                    if m:
                        uid, cp = int(m.group(1)), int(m.group(2))
                        cur.execute(
                            "INSERT INTO users (id, completion_percentage, gender, age) VALUES (%s, %s, 2, 0) ON CONFLICT DO NOTHING",
                            (uid, cp)
                        )
                elif "CREATE (a)-[:Friend]->(b)" in line:
                    ids = re.findall(r'id:\s*(\d+)', line)
                    if len(ids) >= 2:
                        src, dst = int(ids[0]), int(ids[1])
                        cur.execute(
                            "INSERT INTO friends (src_id, dst_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (src, dst)
                        )
                        cur.execute(
                            "INSERT INTO friends (src_id, dst_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (dst, src)
                        )
                elif "DELETE e" in line:
                    ids = re.findall(r'id:(\d+)', line)
                    if len(ids) >= 2:
                        src, dst = int(ids[0]), int(ids[1])
                        cur.execute("DELETE FROM friends WHERE src_id = %s AND dst_id = %s", (src, dst))
                        cur.execute("DELETE FROM friends WHERE src_id = %s AND dst_id = %s", (dst, src))

                if line_num % 10000 == 0:
                    print(f"已处理 {line_num} 行操作")

        # 重置系统时间
        cur.execute("SELECT set_system_time(NULL)")
    conn.commit()

def run_q1(conn, result_file):
    """执行 Q1 查询: 获取指定时间戳的用户属性"""
    result_file.write("\n开始执行Q1查询...\n")
    start = time.time()

    with conn.cursor() as cur:
        with open(f"{DATASET_PATH}/{NUM}/cypher_Q1.txt", "r") as f:
            for line in f:
                m = re.search(r'id\s*:\s*(\d+)', line)
                if m:
                    uid = int(m.group(1))
                    ts = random.randint(1, MAX_TS)
                    cur.execute("SELECT * FROM q1_get_user_at_time(%s, %s)", (uid, ts))
                    cur.fetchall()

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q1查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q2(conn, result_file):
    """执行 Q2 查询: 获取时间范围内的用户属性"""
    result_file.write("\n开始执行Q2查询...\n")
    start = time.time()

    with conn.cursor() as cur:
        with open(f"{DATASET_PATH}/{NUM}/cypher_Q2.txt", "r") as f:
            for line in f:
                m = re.search(r'id\s*:\s*(\d+)', line)
                if m:
                    uid = int(m.group(1))
                    from_ts = random.randint(1, MAX_TS - 100)
                    to_ts = from_ts + 100
                    cur.execute("SELECT * FROM q2_get_user_in_range(%s, %s, %s)", (uid, from_ts, to_ts))
                    cur.fetchall()

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q2查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q3(conn, result_file):
    """执行 Q3 查询: 获取指定时间戳的用户及其好友"""
    result_file.write("\n开始执行Q3查询...\n")
    start = time.time()

    with conn.cursor() as cur:
        with open(f"{DATASET_PATH}/{NUM}/cypher_Q3.txt", "r") as f:
            for line in f:
                m = re.search(r'id\s*:\s*(\d+)', line)
                if m:
                    uid = int(m.group(1))
                    ts = random.randint(1, MAX_TS)
                    cur.execute("SELECT * FROM q3_get_user_friends_at_time(%s, %s)", (uid, ts))
                    cur.fetchall()

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q3查询执行时间: {int(duration)} 微秒\n")
    return duration

def run_q4(conn, result_file):
    """执行 Q4 查询: 获取时间范围内的用户及其好友"""
    result_file.write("\n开始执行Q4查询...\n")
    start = time.time()

    with conn.cursor() as cur:
        with open(f"{DATASET_PATH}/{NUM}/cypher_Q4.txt", "r") as f:
            for line in f:
                m = re.search(r'id\s*:\s*(\d+)', line)
                if m:
                    uid = int(m.group(1))
                    from_ts = random.randint(1, MAX_TS - 100)
                    to_ts = from_ts + 100
                    cur.execute("SELECT * FROM q4_get_user_friends_in_range(%s, %s, %s)", (uid, from_ts, to_ts))
                    cur.fetchall()

    duration = (time.time() - start) * 1000000
    result_file.write(f"Q4查询执行时间: {int(duration)} 微秒\n")
    return duration

def get_table_size(conn, table_name):
    """获取表大小(字节)"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT pg_total_relation_size('{table_name}')")
        return cur.fetchone()[0]

def main():
    conn = get_connection()

    with open(RESULT_FILE, "w") as result_file:
        # 初始化
        print("初始化表结构...")
        setup_schema(conn)

        # 加载初始数据
        print("加载初始数据...")
        start_init = time.time()
        load_initial_data(conn)
        init_duration = (time.time() - start_init) * 1000

        # 加载操作数据
        print("加载操作数据...")
        start_ops = time.time()
        load_operations(conn)
        ops_duration = (time.time() - start_ops) * 1000

        total_load = init_duration + ops_duration
        result_file.write(f"初始数据加载时间: {int(init_duration)} 毫秒\n")
        result_file.write(f"操作数据加载时间: {int(ops_duration)} 毫秒\n")
        result_file.write(f"总加载时间: {int(total_load)} 毫秒\n")

        # 统计内存占用
        users_size = get_table_size(conn, 'users')
        users_hist_size = get_table_size(conn, 'users_history')
        friends_size = get_table_size(conn, 'friends')
        friends_hist_size = get_table_size(conn, 'friends_history')

        current_total = users_size + friends_size
        history_total = users_hist_size + friends_hist_size
        all_total = current_total + history_total

        result_file.write(f"\n内存占用统计:\n")
        result_file.write(f"users表: {users_size/1024/1024:.2f} MB\n")
        result_file.write(f"users_history表: {users_hist_size/1024/1024:.2f} MB\n")
        result_file.write(f"friends表: {friends_size/1024/1024:.2f} MB\n")
        result_file.write(f"friends_history表: {friends_hist_size/1024/1024:.2f} MB\n")
        result_file.write(f"当前数据(非时序): {current_total/1024/1024:.2f} MB ({100*current_total/all_total:.1f}%)\n")
        result_file.write(f"历史数据(时序): {history_total/1024/1024:.2f} MB ({100*history_total/all_total:.1f}%)\n")
        result_file.write(f"总占用: {all_total/1024/1024:.2f} MB\n")

        # 执行查询
        d1 = run_q1(conn, result_file)
        d2 = run_q2(conn, result_file)
        d3 = run_q3(conn, result_file)
        d4 = run_q4(conn, result_file)

        total = d1 + d2 + d3 + d4
        result_file.write(f"\n总执行时间: {int(total)} 微秒\n")

    conn.close()
    print(f"结果已写入 {RESULT_FILE}")

if __name__ == "__main__":
    main()
