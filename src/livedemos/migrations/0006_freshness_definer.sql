-- The freshness view ran with the migrator's rights, which reach every table here. It now
-- runs as freshness_definer (clickhouse/users.d), which can read wiki_edits and write the
-- samples, and nothing else.
ALTER TABLE {database}.freshness_samples_mv MODIFY SQL SECURITY DEFINER DEFINER = freshness_definer;
