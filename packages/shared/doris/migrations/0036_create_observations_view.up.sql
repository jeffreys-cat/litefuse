-- Migration: Create observations view with input hash resolution (ORDER PRESERVED)
--
-- GUARANTEE:
-- This version preserves the original order of hash references in the input array.
-- It uses POSEXPLODE to capture element positions and GROUP_CONCAT with ORDER BY.
--
-- Steps:
-- 1. Rename the existing observations table to observation_source (if it's a TABLE, not VIEW)
-- 2. Create a view "observations" that:
--    - For GENERATION type: resolves input hashes to actual content via content_dict (ORDER PRESERVED)
--    - For non-GENERATION type: passes through input directly

-- Step 1: Check if observations exists as a TABLE (not VIEW) before renaming
-- Only rename if it's a base table, not if it's already a view or doesn't exist
DROP PROCEDURE IF EXISTS safe_rename_observations;

DELIMITER //
CREATE PROCEDURE safe_rename_observations()
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
    ALTER TABLE observations RENAME observation_source;
END//
DELIMITER ;

CALL safe_rename_observations();
DROP PROCEDURE IF EXISTS safe_rename_observations;

-- Step 2: Drop existing observations view if it exists (idempotent)
DROP VIEW IF EXISTS observations;

-- Step 3: Create the observations view using LEFT JOIN for input resolution
-- This approach avoids CAST on output field which was causing escape sequence issues (\n -> n)
CREATE VIEW observations AS

-- Part 1: GENERATION type observations with ordered input reconstruction
-- Uses LEFT JOIN subquery to resolve input hashes while preserving output field as-is
SELECT
    o.project_id,
    o.start_time_date,
    o.id,
    o.type,
    o.trace_id,
    o.parent_observation_id,
    o.start_time,
    o.end_time,
    o.name,
    o.metadata,
    o.level,
    o.status_message,
    o.version,
    CAST(t.input_resolved AS VARIANT) as input,
    o.output,
    o.provided_model_name,
    o.internal_model_id,
    o.model_parameters,
    o.provided_usage_details,
    o.usage_details,
    o.provided_cost_details,
    o.cost_details,
    o.total_cost,
    o.completion_start_time,
    o.prompt_id,
    o.prompt_name,
    o.prompt_version,
    o.created_at,
    o.updated_at,
    o.event_ts,
    o.is_deleted,
    o.environment,
    o.usage_pricing_tier_id,
    o.usage_pricing_tier_name,
    o.tool_definitions,
    o.tool_calls,
    o.tool_call_names
FROM observation_source o
LEFT JOIN (
    SELECT
        o_inner.id,
        CONCAT(
            '[',
            GROUP_CONCAT(CAST(c.content AS VARCHAR) ORDER BY t_inner.pos),
            ']'
        ) as input_resolved
    FROM observation_source o_inner
    LATERAL VIEW POSEXPLODE(CAST(o_inner.input AS ARRAY<VARCHAR>)) t_inner AS pos, hash_item
    LEFT JOIN content_dict c
        ON t_inner.hash_item = c.content_hash
    WHERE o_inner.type = 'GENERATION'
      AND o_inner.input IS NOT NULL
    GROUP BY o_inner.id
) t
ON o.id = t.id
WHERE o.type = 'GENERATION' AND o.input IS NOT NULL

UNION ALL

-- Part 2: Non-GENERATION type observations (pass-through)
SELECT
    project_id,
    start_time_date,
    id,
    type,
    trace_id,
    parent_observation_id,
    start_time,
    end_time,
    name,
    metadata,
    level,
    status_message,
    version,
    input,
    output,
    provided_model_name,
    internal_model_id,
    model_parameters,
    provided_usage_details,
    usage_details,
    provided_cost_details,
    cost_details,
    total_cost,
    completion_start_time,
    prompt_id,
    prompt_name,
    prompt_version,
    created_at,
    updated_at,
    event_ts,
    is_deleted,
    environment,
    usage_pricing_tier_id,
    usage_pricing_tier_name,
    tool_definitions,
    tool_calls,
    tool_call_names
FROM observation_source
WHERE type != 'GENERATION' OR input IS NULL;