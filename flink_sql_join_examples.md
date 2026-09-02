# Flink SQL JOIN 类型示例文档

本文档提供了在 Waterdrop 框架中使用 SQL Transform 插件实现各种 Flink SQL JOIN 类型的示例。

## 1. 常规 JOIN（内连接）

最基本的 JOIN 类型，只返回两个表中匹配的记录。

```sql
SELECT a.id, a.name, b.score
FROM orders a
JOIN users b
ON a.user_id = b.id
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "joined_result"
    sql = """
      SELECT a.id, a.name, b.score
      FROM orders a
      JOIN users b
      ON a.user_id = b.id
    """
  }
}
```

## 2. 外连接（LEFT/RIGHT/FULL OUTER JOIN）

### 2.1 LEFT OUTER JOIN

保留左表所有记录，右表不匹配的字段为 NULL。

```sql
SELECT a.id, a.name, b.score
FROM orders a
LEFT JOIN users b
ON a.user_id = b.id
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "left_joined_result"
    sql = """
      SELECT a.id, a.name, b.score
      FROM orders a
      LEFT JOIN users b
      ON a.user_id = b.id
    """
  }
}
```

### 2.2 RIGHT OUTER JOIN

保留右表所有记录，左表不匹配的字段为 NULL。

```sql
SELECT a.id, a.name, b.score
FROM orders a
RIGHT JOIN users b
ON a.user_id = b.id
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "right_joined_result"
    sql = """
      SELECT a.id, a.name, b.score
      FROM orders a
      RIGHT JOIN users b
      ON a.user_id = b.id
    """
  }
}
```

### 2.3 FULL OUTER JOIN

保留两个表中的所有记录，不匹配的字段为 NULL。

```sql
SELECT a.id, a.name, b.score
FROM orders a
FULL OUTER JOIN users b
ON a.user_id = b.id
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "full_joined_result"
    sql = """
      SELECT a.id, a.name, b.score
      FROM orders a
      FULL OUTER JOIN users b
      ON a.user_id = b.id
    """
  }
}
```

## 3. 基于时间窗口的 JOIN（Interval Join）

匹配在指定时间范围内的记录。

```sql
SELECT a.id, a.user_id, b.item_name
FROM orders a
JOIN shipments b
ON a.id = b.order_id
AND b.ship_time BETWEEN a.order_time AND a.order_time + INTERVAL '4' HOUR
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "interval_joined_result"
    sql = """
      SELECT a.id, a.user_id, b.item_name
      FROM orders a
      JOIN shipments b
      ON a.id = b.order_id
      AND b.ship_time BETWEEN a.order_time AND a.order_time + INTERVAL '4' HOUR
    """
  }
}
```

## 4. 基于处理时间的 JOIN

使用系统处理时间进行 JOIN。

### 4.1 表定义（需要在 Flink 中创建）

```sql
CREATE TABLE orders (
    id STRING,
    user_id STRING,
    amount DOUBLE,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)

CREATE TABLE order_status (
    order_id STRING,
    status STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_status',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
```

### 4.2 处理时间 JOIN 查询

```sql
SELECT a.id, a.user_id, b.status
FROM orders a
JOIN order_status b
ON a.id = b.order_id
AND b.proc_time BETWEEN a.proc_time - INTERVAL '1' MINUTE AND a.proc_time
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "proctime_joined_result"
    sql = """
      SELECT a.id, a.user_id, b.status
      FROM orders a
      JOIN order_status b
      ON a.id = b.order_id
      AND b.proc_time BETWEEN a.proc_time - INTERVAL '1' MINUTE AND a.proc_time
    """
  }
}
```

## 5. 基于事件时间的 JOIN

使用数据中的时间戳字段进行 JOIN。

### 5.1 表定义（需要在 Flink 中创建）

```sql
CREATE TABLE orders (
    id STRING,
    user_id STRING,
    amount DOUBLE,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)

CREATE TABLE payments (
    id STRING,
    order_id STRING,
    payment_time TIMESTAMP(3),
    WATERMARK FOR payment_time AS payment_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
```

### 5.2 事件时间 JOIN 查询

```sql
SELECT o.id, o.amount, p.id AS payment_id
FROM orders o
JOIN payments p
ON o.id = p.order_id
AND p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '15' MINUTE
```

**Waterdrop 配置示例：**

```hocon
transform {
  sql {
    source_table_name = "orders"
    result_table_name = "eventtime_joined_result"
    sql = """
      SELECT o.id, o.amount, p.id AS payment_id
      FROM orders o
      JOIN payments p
      ON o.id = p.order_id
      AND p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '15' MINUTE
    """
  }
}
```

## 6. 完整的 Waterdrop 配置示例

下面是一个完整的 Waterdrop 配置示例，包含多个 JOIN 操作：

```hocon
env {
  # 设置 Flink 流处理环境
  execution.parallelism = 2
  job.name = "Flink SQL JOIN Examples"
  
  # 设置事件时间特性
  time.characteristic = "event-time"
  
  # 设置检查点和状态后端
  checkpoint.interval = 60000
  state.backend = "rocksdb"
  checkpoint.data.uri = "hdfs://namenode:8020/flink/checkpoints"
  
  # 设置状态的 TTL
  max.state.retention.time = 3600000
  min.state.retention.time = 1800000
}

source {
  # 定义订单数据源
  kafka {
    result_table_name = "orders"
    topics = "orders"
    consumer.bootstrap.servers = "kafka:9092"
    consumer.group.id = "waterdrop-flink"
    format = "json"
    schema = {
      fields {
        id = "string"
        user_id = "string"
        amount = "double"
        order_time = "timestamp"
      }
      watermark = [{
        field = "order_time"
        strategy = "bounded_out_of_orderness"
        delay = "5000"
      }]
    }
  }
  
  # 定义用户数据源
  kafka {
    result_table_name = "users"
    topics = "users"
    consumer.bootstrap.servers = "kafka:9092"
    consumer.group.id = "waterdrop-flink"
    format = "json"
    schema = {
      fields {
        id = "string"
        name = "string"
        score = "int"
      }
    }
  }
  
  # 定义支付数据源
  kafka {
    result_table_name = "payments"
    topics = "payments"
    consumer.bootstrap.servers = "kafka:9092"
    consumer.group.id = "waterdrop-flink"
    format = "json"
    schema = {
      fields {
        id = "string"
        order_id = "string"
        payment_time = "timestamp"
      }
      watermark = [{
        field = "payment_time"
        strategy = "bounded_out_of_orderness"
        delay = "5000"
      }]
    }
  }
}

transform {
  # 内连接示例
  sql {
    source_table_name = "orders"
    result_table_name = "inner_join_result"
    sql = """
      SELECT a.id, a.user_id, a.amount, b.name
      FROM orders a
      JOIN users b
      ON a.user_id = b.id
    """
  }
  
  # 事件时间 JOIN 示例
  sql {
    source_table_name = "orders"
    result_table_name = "time_join_result"
    sql = """
      SELECT o.id, o.amount, p.id AS payment_id
      FROM orders o
      JOIN payments p
      ON o.id = p.order_id
      AND p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '15' MINUTE
    """
  }
}

sink {
  # 将结果写入 Kafka
  kafka {
    source_table_name = "inner_join_result"
    producer.bootstrap.servers = "kafka:9092"
    topic = "join_results"
    format = "json"
  }
  
  # 将时间 JOIN 结果写入 Kafka
  kafka {
    source_table_name = "time_join_result"
    producer.bootstrap.servers = "kafka:9092"
    topic = "time_join_results"
    format = "json"
  }
}
```

## 7. 注意事项

1. **状态管理**：流式 JOIN 会产生状态，需要设置合适的状态后端和检查点机制。

2. **状态清理**：设置适当的状态 TTL（Time-To-Live）以避免状态无限增长。

3. **水印配置**：对于事件时间 JOIN，正确配置水印生成策略至关重要。

4. **资源配置**：JOIN 操作可能消耗大量资源，需要合理配置并行度和内存。

5. **监控**：实际生产环境中，应监控 JOIN 操作的状态大小和处理延迟。 