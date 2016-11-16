class UseDashOnJsonapiSo < ActiveRecord::Migration
  def up
    JSONAPI.service.setup
  end

  def down
    puts "NOT REVERTING FUNCTION DECLARATION!"
  end
end
