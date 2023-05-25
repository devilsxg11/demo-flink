-- Define available catalogs

CREATE CATALOG hiveCatalog
  WITH (
    'type' = 'hive',
    'default-database' = 'tmp',
    'hive-conf-dir' = '/app/hive/conf'
  );

USE CATALOG hiveCatalog;

-- Define available database

USE tmp;

-- Define TABLE
DROP TABLE IF EXISTS sxg_sql_client_events;

CREATE TABLE sxg_sql_client_events (
  distinct_id STRING,
  event STRING,
  proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'event_topic',
  'properties.bootstrap.servers' = '10.115.4.144:9092,10.115.4.182:9092,10.115.4.183:9092',
  'properties.group.id' = 'sxg_event_topic_test_1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);


DROP TABLE IF EXISTS sxg_sql_client_result;

CREATE TABLE sxg_sql_client_result(
  event STRING,
  cnt BIGINT,
  PRIMARY KEY (event) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://10.115.88.138:3306/test?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false',
  'table-name' = 'events',
  'username' = 'root',
  'password' = 'root.123'
);



-- set sync mode
SET 'table.dml-sync' = 'true';

-- set the job name
SET 'pipeline.name' = 'sxg_sql_job';

-- set the queue that the job submit to
SET 'yarn.application.queue' = 'root';

-- set the job parallelism
SET 'parallelism.default' = '10';

INSERT INTO sxg_sql_client_result
SELECT event,count(distinct distinct_id)
FROM sxg_sql_client_events
GROUP BY event;


