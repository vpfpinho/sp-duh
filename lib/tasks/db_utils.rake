require 'yaml'
require 'pg'

def load_db_from_yml_spec (a_spec)
  a_spec["folders"].each do |elem|
    Pathname.glob("#{MOD_PATH}/db/#{elem}/**/").sort.each do |folder|
      Dir["#{folder}/*.sql"].sort.each do |file|
        puts "file #{file}"
        $db.exec( File.read(file) )
      end
    end
  end

  return if a_spec["execute"].nil?

  a_spec["folders"].each do |folder|
    next unless a_spec["execute"].has_key?(folder)
    next unless a_spec["execute"][folder].is_a?(Array)
    a_spec["execute"][folder].each do |to_execute|
      puts to_execute
      $db.exec( "SELECT #{ to_execute};" )
    end
  end
end

def load_db_config
  ENV['RAILS_ENV'] ||= 'development'
  $db_config = YAML.load_file(File.join('config', 'database.yml'))[ENV['RAILS_ENV']]
end

def connect_to_pg
  load_db_config()
  $db = PG.connect(host: $db_config['host'], port: $db_config['port'], dbname: $db_config['database'], user: $db_config['username'], password: $db_config['password'])
end

def config_json_api
  jsonapi_service = SP::Duh::JSONAPI::Service.new($db, JSONAPI_PREFIX)
  jsonapi_service.setup()
  jsonapi_service.configuration.reload!
end

task :pg_connect do
  $db.close unless $db.nil?
  connect_to_pg()
  $db
end

desc 'Reload jsonapi'
task :config_jsonapi => :pg_connect do
  config_json_api()
end

task :production_safety do
  load_db_config()
  unless Object.const_defined?('FORBIDDEN_HOSTS')
    raise "To run this task you must define 'FORBIDDEN_HOSTS' No, you don't know what you are doing!!!!"
  end
  if FORBIDDEN_HOSTS.include? %x[hostname -s].strip
    raise "For safety reasons this task can't be run on this machine, no you don't know what you are doing"
  end
  unless %w(localhost tocstaging cloudex 127.0.0.1).include? $db_config['host']
    raise "For safety reasons database host #{$db_config['host']} is not allowed"
  end
end

desc 'Reload GEM functions defined in db_functions.yml (FOR LOCAL GEM ONLY)'
task :reload_functions => [:production_safety, :pg_connect] do
  unless Object.const_defined?('MOD_PATH')
    raise "To run this task you must define 'MOD_PATH'!"
  end
  load_db_from_yml_spec(YAML.load_file(File.join(MOD_PATH, 'config', 'db_functions.yml')))
end

desc 'Create a new database seed using db_seed.yml spec'
task :create_db => :production_safety do

  begin
    connect_to_pg()
    unless $db.nil?
      $db.close
      nuke_db = ask("Are you sure? The current database #{$db_config['database']} on #{$db_config['host']} will be destroyed!!!!") { |q| q.default = 'no' }
      if nuke_db.downcase == 'yes'
        %x[dropdb -p #{$db_config['port']} -U #{$db_config['username']} -h #{$db_config['host']} #{$db_config['database']}]
        raise 'dropdb failed, bailing out' unless $?.success?
      end
    end
  rescue
  end

  %x[PGPASSWORD=#{$db_config['password']} createdb -h #{$db_config['host']} -U #{$db_config['username']} #{$db_config['database']}]
  raise 'createdb failed, bailing out' unless $?.success?
  connect_to_pg()
  load_db_from_yml_spec(YAML.load_file(File.join(MOD_PATH, 'config', 'db_seed.yml')))
  load_db_from_yml_spec(YAML.load_file(File.join(MOD_PATH, 'config', 'db_functions.yml')))
  config_json_api()
end
