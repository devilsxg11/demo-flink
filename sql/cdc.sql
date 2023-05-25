
SET 'execution.checkpointing.interval' = '300s';

-- 使用 HiveCatalog 存储元信息
CREATE CATALOG hiveCatalog
  WITH (
    'type' = 'hive',
    'default-database' = 'tmp',
    'hive-conf-dir' = '/app/hive/conf'
  );

USE CATALOG hiveCatalog;

-- 使用 tmp 库
USE tmp;

-- 使用 CDC Connector 定义源表
DROP TABLE IF EXISTS sxg_cdc_order;

CREATE TABLE sxg_cdc_order (
                               order_id BIGINT,
                               amount BIGINT,
                               channel_id INT,
                               PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '10.112.182.11',
      'port' = '3306',
      'username' = 'data_api_user',
      'password' = 'api_use68pw9esd',
      'database-name' = 'test',
      'table-name' = 'sxg_order'
      );

-- 使用 Table Connector 定义目标表
DROP TABLE IF EXISTS sxg_cdc_order_sum;

CREATE TABLE sxg_cdc_order_sum(
                                  channel_id INT,
                                  sum_amount BIGINT,
                                  update_time AS PROCTIME(),
                                  PRIMARY KEY (channel_id) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://10.112.182.11:3306/test?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false',
      'table-name' = 'sxg_order_sum',
      'username' = 'data_api_user',
      'password' = 'api_use68pw9esd'
      );


-- 将会总后的数据写入订单汇总表
INSERT INTO sxg_cdc_order_sum
SELECT channel_id,sum(amount)
FROM sxg_cdc_order
GROUP BY channel_id;

