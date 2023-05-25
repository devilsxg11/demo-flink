
-- 设置 job 名称
SET 'pipeline.name' = 'sxg_cdc_job';
-- 设置 checkpoint 
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
      'username' = 'root',
      'password' = 'root_use68pw9esd',
      'database-name' = 'test',
      'table-name' = 'sxg_order',
      'server-id' = '1'	
      );

-- 使用 Table Connector 定义目标表
DROP TABLE IF EXISTS sxg_cdc_order_sum;

CREATE TABLE sxg_cdc_order_sum(
                                  channel_id INT,
                                  sum_amount BIGINT,
                                  update_time TIMESTAMP(3),
                                  PRIMARY KEY (channel_id) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://10.112.182.11:3306/test?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false',
      'table-name' = 'sxg_order_sum',
      'username' = 'root',
      'password' = 'root_use68pw9esd'
      );


-- 将会总后的数据写入订单汇总表
INSERT INTO sxg_cdc_order_sum
SELECT channel_id,sum(amount),LOCALTIMESTAMP
FROM sxg_cdc_order
GROUP BY channel_id;

