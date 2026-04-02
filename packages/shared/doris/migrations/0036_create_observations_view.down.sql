-- Revert migration: Remove observations view and restore original table
-- This migration:
-- 1. Drops the observations view
-- 2. Restores the observation_source table back to observations

-- Step 1: Drop the observations view
DROP VIEW IF EXISTS observations;

-- Step 2: Restore the original table name (only if observation_source is a TABLE)
DROP PROCEDURE IF EXISTS safe_rename_back;

DELIMITER //
CREATE PROCEDURE safe_rename_back()
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
    ALTER TABLE observation_source RENAME observations;
END//
DELIMITER ;

CALL safe_rename_back();
DROP PROCEDURE IF EXISTS safe_rename_back;