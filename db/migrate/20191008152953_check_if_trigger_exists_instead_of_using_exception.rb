class CheckIfTriggerExistsInsteadOfUsingException < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      -- DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT, TEXT, TEXT);
      CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
        IN p_query        TEXT,
        IN p_table_name   TEXT,
        IN p_trigger_name TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
      BEGIN
        RETURN format(
          $RETURN$
            DO $BLOCK$
              BEGIN
                IF NOT sharding.trigger_exists('%2$s','%3$s') THEN
                  %1$s
                END IF;
              END;
            $BLOCK$
          $RETURN$,
          p_query, p_table_name, p_trigger_name
        );
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    puts "not reverting to code which locks tables...".red
  end
end
