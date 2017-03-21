class CleanRedisStoredProcedures < ActiveRecord::Migration
  def up
    execute %Q[DROP FUNCTION IF EXISTS redis.create_redis_server_connection(TEXT, TEXT, INTEGER, INTEGER);]
    execute %Q[DROP FUNCTION IF EXISTS redis.create_trigger_to_delete_cache_keys(TEXT, TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS redis.delete_redis_server_connection(TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS redis.delete_trigger_to_delete_cache_keys(TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS redis.trf_delete_affected_optimizations_cache_entries();]
    execute %Q[DROP FUNCTION IF EXISTS redis.trf_mark_optimizations_cache_entries_for_deletion();]

    execute %Q[DROP SCHEMA IF EXISTS redis;]
  end

  def down
    say "Computer says no!"
  end
end
