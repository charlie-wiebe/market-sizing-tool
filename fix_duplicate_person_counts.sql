-- Fix Duplicate Active Person Counts
-- This script deactivates duplicate person count records, keeping only the most recent one per company/query_name

-- 1. First, check the scope of the problem
SELECT 
    query_name, 
    COUNT(DISTINCT company_id) as affected_companies, 
    COUNT(*) as total_duplicate_records,
    SUM(CASE WHEN query_name = 'SDR Count' THEN 1 ELSE 0 END) as sdr_duplicates,
    SUM(CASE WHEN query_name = 'Sales Team Count (excl. SDR)' THEN 1 ELSE 0 END) as sales_team_duplicates
FROM person_counts
WHERE is_active = true
AND query_name IN ('SDR Count', 'Sales Team Count (excl. SDR)')
GROUP BY query_name
HAVING COUNT(*) > (SELECT COUNT(DISTINCT company_id) FROM person_counts WHERE query_name = person_counts.query_name AND is_active = true);

-- 2. Show sample of duplicates before fix
SELECT 
    pc.company_id,
    c.name as company_name,
    pc.query_name,
    COUNT(*) as duplicate_count,
    STRING_AGG(CAST(pc.id AS TEXT), ', ' ORDER BY pc.created_at DESC) as person_count_ids,
    STRING_AGG(CAST(pc.total_count AS TEXT), ', ' ORDER BY pc.created_at DESC) as count_values,
    STRING_AGG(TO_CHAR(pc.created_at, 'YYYY-MM-DD HH24:MI:SS'), ', ' ORDER BY pc.created_at DESC) as created_dates
FROM person_counts pc
JOIN companies c ON c.id = pc.company_id
WHERE pc.is_active = true
AND pc.query_name IN ('SDR Count', 'Sales Team Count (excl. SDR)')
GROUP BY pc.company_id, c.name, pc.query_name
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 3. MAIN FIX: Deactivate all but the most recent record per company/query_name
UPDATE person_counts
SET is_active = false
WHERE id IN (
    SELECT id FROM (
        SELECT 
            id,
            ROW_NUMBER() OVER (PARTITION BY company_id, query_name ORDER BY created_at DESC) as rn
        FROM person_counts
        WHERE is_active = true
        AND query_name IN ('SDR Count', 'Sales Team Count (excl. SDR)')
    ) ranked
    WHERE rn > 1
);

-- 4. Verify the fix worked - this should return 0 rows
SELECT 
    company_id, 
    query_name, 
    COUNT(*) as active_count
FROM person_counts
WHERE is_active = true
AND query_name IN ('SDR Count', 'Sales Team Count (excl. SDR)')
GROUP BY company_id, query_name
HAVING COUNT(*) > 1;

-- 5. Show summary after fix
SELECT 
    query_name,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(*) as active_records,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT company_id) THEN 'FIXED - No duplicates'
        ELSE 'STILL HAS DUPLICATES!'
    END as status
FROM person_counts
WHERE is_active = true
AND query_name IN ('SDR Count', 'Sales Team Count (excl. SDR)')
GROUP BY query_name;
