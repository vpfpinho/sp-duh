class FixSterlingTransalationPt < ActiveRecord::Migration
  def up
    execute <<-SQL
      UPDATE public.i18n SET pt='libra esterlina'   WHERE key = 'gbp_major_singular';
      UPDATE public.i18n SET pt='libras esterlinas' WHERE key = 'gbp_major_plural';
    SQL
  end

  def down
    puts 'No down for you today'
  end
end
