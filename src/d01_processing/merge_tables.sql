-- Get a list of tables in a specific schema
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'nfhl_tr32.S_FLD_HAZ_AR_temp'
  AND table_type = 'BASE TABLE';

-- Get columns for each table in the schema
SELECT table_name, array_agg(column_name) AS columns
FROM information_schema.columns
WHERE table_schema = 'S_FLD_HAZ_AR_temp'
GROUP BY table_name;
