-- PostgreSQL MyBench 时序查询实验 (num=32)
-- 使用 temporal_tables 扩展支持时序查询

-- 启用扩展
CREATE EXTENSION IF NOT EXISTS temporal_tables;

-- 删除旧表
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS users_history CASCADE;
DROP TABLE IF EXISTS friends CASCADE;
DROP TABLE IF EXISTS friends_history CASCADE;

-- 删除旧函数
DROP FUNCTION IF EXISTS line_to_ts(INTEGER);
DROP FUNCTION IF EXISTS q1_get_user_at_time(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS q2_get_user_in_range(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS q3_get_user_friends_at_time(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS q4_get_user_friends_in_range(INTEGER, INTEGER, INTEGER);

-- 创建用户表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    completion_percentage INTEGER,
    gender INTEGER,  -- 1=man, 0=woman
    age INTEGER,
    sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
);

-- 创建用户历史表
CREATE TABLE users_history (LIKE users);

-- 创建好友关系表
CREATE TABLE friends (
    src_id INTEGER,
    dst_id INTEGER,
    sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null),
    PRIMARY KEY (src_id, dst_id)
);

-- 创建好友历史表
CREATE TABLE friends_history (LIKE friends);

-- 创建时序触发器
CREATE TRIGGER users_versioning
BEFORE INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'users_history', true);

CREATE TRIGGER friends_versioning
BEFORE INSERT OR UPDATE OR DELETE ON friends
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'friends_history', true);

-- 创建索引
CREATE INDEX idx_users_history_id ON users_history(id);
CREATE INDEX idx_users_history_period ON users_history USING gist(sys_period);
CREATE INDEX idx_friends_history_src ON friends_history(src_id);
CREATE INDEX idx_friends_history_period ON friends_history USING gist(sys_period);

-- 辅助函数：将行号转换为时间戳（模拟时序）
CREATE OR REPLACE FUNCTION line_to_ts(line_num INTEGER) RETURNS timestamptz AS $$
BEGIN
    RETURN '2020-01-01 00:00:00'::timestamptz + (line_num || ' seconds')::interval;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Q1: 获取指定时间戳的用户属性
CREATE OR REPLACE FUNCTION q1_get_user_at_time(p_id INTEGER, p_ts INTEGER)
RETURNS TABLE(id INTEGER, completion_percentage INTEGER, gender INTEGER, age INTEGER) AS $$
DECLARE
    ts_val timestamptz := line_to_ts(p_ts);
BEGIN
    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age
    FROM (
        SELECT * FROM users WHERE users.id = p_id AND sys_period @> ts_val
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = p_id AND sys_period @> ts_val
    ) u LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Q2: 获取时间范围内的所有版本
CREATE OR REPLACE FUNCTION q2_get_user_in_range(p_id INTEGER, p_from_ts INTEGER, p_to_ts INTEGER)
RETURNS TABLE(id INTEGER, completion_percentage INTEGER, gender INTEGER, age INTEGER, sys_period tstzrange) AS $$
DECLARE
    query_range tstzrange := tstzrange(line_to_ts(p_from_ts), line_to_ts(p_to_ts), '[]');
BEGIN
    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age, u.sys_period
    FROM (
        SELECT * FROM users WHERE users.id = p_id AND users.sys_period && query_range
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = p_id AND users_history.sys_period && query_range
    ) u ORDER BY u.sys_period;
END;
$$ LANGUAGE plpgsql;

-- Q3: 获取指定时间戳的用户及其好友
CREATE OR REPLACE FUNCTION q3_get_user_friends_at_time(p_id INTEGER, p_ts INTEGER)
RETURNS TABLE(user_id INTEGER, completion_percentage INTEGER, gender INTEGER, age INTEGER, is_source BOOLEAN) AS $$
DECLARE
    ts_val timestamptz := line_to_ts(p_ts);
BEGIN
    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age, true
    FROM (
        SELECT * FROM users WHERE users.id = p_id AND sys_period @> ts_val
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = p_id AND sys_period @> ts_val
    ) u LIMIT 1;

    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age, false
    FROM (
        SELECT dst_id FROM friends WHERE src_id = p_id AND sys_period @> ts_val
        UNION ALL
        SELECT dst_id FROM friends_history WHERE src_id = p_id AND sys_period @> ts_val
    ) f
    JOIN LATERAL (
        SELECT * FROM users WHERE users.id = f.dst_id AND sys_period @> ts_val
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = f.dst_id AND sys_period @> ts_val
        LIMIT 1
    ) u ON true;
END;
$$ LANGUAGE plpgsql;

-- Q4: 获取时间范围内的用户及其好友的所有版本
CREATE OR REPLACE FUNCTION q4_get_user_friends_in_range(p_id INTEGER, p_from_ts INTEGER, p_to_ts INTEGER)
RETURNS TABLE(user_id INTEGER, completion_percentage INTEGER, gender INTEGER, age INTEGER, sys_period tstzrange) AS $$
DECLARE
    query_range tstzrange := tstzrange(line_to_ts(p_from_ts), line_to_ts(p_to_ts), '[]');
BEGIN
    -- 源用户的所有版本
    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age, u.sys_period
    FROM (
        SELECT * FROM users WHERE users.id = p_id AND users.sys_period && query_range
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = p_id AND users_history.sys_period && query_range
    ) u;

    -- 好友的所有版本
    RETURN QUERY
    SELECT u.id, u.completion_percentage, u.gender, u.age, u.sys_period
    FROM (
        SELECT DISTINCT dst_id FROM friends WHERE src_id = p_id AND friends.sys_period && query_range
        UNION
        SELECT DISTINCT dst_id FROM friends_history WHERE src_id = p_id AND friends_history.sys_period && query_range
    ) f
    JOIN LATERAL (
        SELECT * FROM users WHERE users.id = f.dst_id AND users.sys_period && query_range
        UNION ALL
        SELECT * FROM users_history WHERE users_history.id = f.dst_id AND users_history.sys_period && query_range
    ) u ON true;
END;
$$ LANGUAGE plpgsql;
