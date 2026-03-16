-- ============================================
-- CREATE NEW DATABASE
-- ============================================

SET @TARGET_DB = 'VAMA';
SET @SOURCE_DB = 'AJMAL';

SET @sql = CONCAT('CREATE DATABASE IF NOT EXISTS ', @TARGET_DB);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- ============================================
-- CLONE ALL TABLES
-- ============================================

SELECT
CONCAT(
'CREATE TABLE ',
@TARGET_DB,
'.',
table_name,
' LIKE ',
@SOURCE_DB,
'.',
table_name,
';'
)

FROM information_schema.tables
WHERE table_schema = @SOURCE_DB;