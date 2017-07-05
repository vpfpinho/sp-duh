class AddFunctionArraySum < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.array_sum (
          IN a_array         numeric[]
      )
      RETURNS numeric AS $$
        SELECT sum( COALESCE(a_array[i],0) )::numeric FROM generate_series( array_lower(a_array,1), array_upper(a_array,1) ) index(i);
      $$ LANGUAGE 'sql' STABLE;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION common.array_sum(numeric[]);
    SQL
  end
end
