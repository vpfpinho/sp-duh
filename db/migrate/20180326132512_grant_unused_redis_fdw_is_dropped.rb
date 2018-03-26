class GrantUnusedRedisFdwIsDropped < ActiveRecord::Migration
  def up
    execute %Q[DROP EXTENSION IF EXISTS redis_fdw;]
  end

  def down
    say "Computer says no!"
  end
end
