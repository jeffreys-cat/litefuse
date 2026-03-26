CREATE TABLE IF NOT EXISTS content_dict (
    content_hash CHAR(64)  NOT NULL COMMENT 'SHA-256(content)',
    content      VARIANT      NOT NULL COMMENT 'Actual text content'
) ENGINE = OLAP
UNIQUE KEY (content_hash)
DISTRIBUTED BY HASH(content_hash) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);
