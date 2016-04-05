require 'sinatra'
require 'json'
require 'yaml'
require 'pg'
require 'byebug'
require 'net/http'

# Required for extra methods added to Ruby types, such as the .blank? method of String
require 'active_support'
require 'active_support/core_ext'

require File.join(File.expand_path('../..', File.dirname(__FILE__)), 'lib', 'sp', 'duh', 'jsonapi', 'service')
require File.join(File.expand_path('../..', File.dirname(__FILE__)), 'lib', 'sp', 'duh', 'jsonapi', 'adapters', 'base')
require File.join(File.expand_path('../..', File.dirname(__FILE__)), 'lib', 'sp', 'duh', 'jsonapi', 'adapters', 'raw_db')

# Bundler.require

configuration = YAML.load_file(File.join(File.expand_path(File.dirname(__FILE__)), 'config.yml'))

server_configuration = configuration['server'] || {}
pg_configuration = configuration['database'] || {}
jsonapi_configuration = configuration['jsonapi'] || {}

set :logging, true
set :port, server_configuration['port'] || 9001

pg_connection = PG.connect({
    host: pg_configuration['host'],
    port: pg_configuration['port'],
    dbname: pg_configuration['database'],
    user: pg_configuration['username'],
    password: pg_configuration['password']
})

url = jsonapi_configuration['url'] || "http://localhost:#{server_configuration['port'] || 9001}"

jsonapi_service = SP::Duh::JSONAPI::Service.new(pg_connection, url)
$jsonapi_adapter = SP::Duh::JSONAPI::Adapters::RawDb.new(jsonapi_service)

get '/*' do
  process_request
end

post '/*' do
  process_request
end

put '/*' do
  process_request
end

patch '/*' do
  process_request
end

delete '/*' do
  process_request
end

def process_request
  content_type 'application/vnd.api+json', :charset => 'utf-8'
  begin
    # Send the sharding parameters in the requesr headers
    schema = request.env['HTTP_X_JSONAPI_SCHEMA'] || ''
    prefix = request.env['HTTP_X_JSONAPI_PREFIX'] || ''
    $jsonapi_adapter.request!(request.fullpath, schema, prefix, request.body.read, request.request_method.upcase)
  rescue Exception => e
    puts e
    puts e.backtrace
    [ 500,
      {
        errors: [ {status: '500', code: e.message } ],
        links: { self: request.url },
        jsonapi: { version: '1.0' }
      }.to_json
    ]
  end
end

