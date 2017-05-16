class CreateFunctionInsideJsonapi < ActiveRecord::Migration
  def up
    execute %Q[
      CREATE OR REPLACE FUNCTION inside_jsonapi (
      ) RETURNS boolean AS '$libdir/pg-jsonapi.so', 'inside_jsonapi' LANGUAGE C;
    ]
  end

  def down
    execute %Q[
      DROP FUNCTION IF EXISTS inside_jsonapi ()
    ]
  end
end
