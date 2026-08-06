-- SPLIT TEMPLATE — content_dict_<pid> (per-project). This file intentionally
-- has no .up.sql suffix, so scripts/up.sh never creates a shared table.
-- buildSplitTableStatements replaces __TABLE__ and adds the dynamic-partition
-- and distribution tail.

CREATE TABLE IF NOT EXISTS __TABLE__ (
    `start_time` DATE NOT NULL COMMENT 'UTC day of the matching events_full row',
    `content_hash` CHAR(64) NOT NULL COMMENT 'SHA-256(content)',
    `content` String NOT NULL COMMENT 'Serialized input element or scalar text',
    INDEX idx_content (`content`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT 'full-text index for input content search'
) ENGINE = OLAP
UNIQUE KEY (`start_time`, `content_hash`)
