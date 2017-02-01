class QualifyFunctionLastDayOnPublic < ActiveRecord::Migration
  def up
		execute <<-'SQL'
			CREATE OR REPLACE FUNCTION public.last_day(DATE)
			RETURNS DATE AS
			$$
				SELECT (date_trunc('MONTH', $1) + INTERVAL '1 MONTH - 1 day')::DATE;
			$$ LANGUAGE 'sql' IMMUTABLE STRICT;
		SQL
  end

  def down
		execute <<-'SQL'
			CREATE OR REPLACE FUNCTION last_day(DATE)
			RETURNS DATE AS
			$$
				SELECT (date_trunc('MONTH', $1) + INTERVAL '1 MONTH - 1 day')::DATE;
			$$ LANGUAGE 'sql' IMMUTABLE STRICT;
		SQL
  end
end

