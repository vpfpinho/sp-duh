require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'version'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'engine'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'repl'))

# JSONAPI library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'resource_publisher'))
# Service classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'service'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'configuration'))
# Adpater classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'base'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'raw_db'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'db'))
# PG Adapters
require 'pg'
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'adapters', 'pg', 'text_decoder', 'json'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'adapters', 'pg', 'text_encoder', 'json'))

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'model', 'base'))

# Migrations library classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'migrations'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'migrations', 'migrator'))

# Backup and restore (transfer) library classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'db', 'transfer', 'backup'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'db', 'transfer', 'restore'))

# SP helper lib
require 'sp-excel-loader'

module SP
  module Duh
    def self.root
      File.expand_path '../../..', __FILE__
    end

    def self.mock_pdf (args)
      jrxml_base = File.basename("#{args[:name]}")
      Dir.mkdir './tmp' unless Dir.exists?('./tmp')
      converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new("config/#{args[:name]}.xlsx", nil, true, true, false)
      File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}_compat.jrxml")
      converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new("config/#{args[:name]}.xlsx", nil, true, true, true)
      File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}.jrxml")

      return unless args.has_key?(:data)

      control = [{
          :locale           => 'pt-PT',
          :template_file    => File.expand_path("./tmp/#{jrxml_base}.jrxml"),
          :report_file      => File.expand_path("./tmp/#{jrxml_base}.jrxml"),
          :data_file        => File.expand_path('./tmp/data.json'),
          :number_of_copies => 1,
          :auto_printable   => false
        }]
      if args.has_key?(:print_server) and args[:print_server] == 'false'
        File.write(File.expand_path('./tmp/control.json'), control.to_json)
        File.write(File.expand_path('./tmp/data.json'), args[:data])
        result = JSON.parse(%x[ /usr/local/bin/casper-print -j #{File.expand_path('./tmp/control.json')} ])
        if result['file']
          %x[open #{result['file']}]
        else
          ap result
        end
      else
        # Use legacy format to talk with print server
        uri           = URI('http://localhost:3001/print')
        http          = Net::HTTP.new(uri.host, uri.port)
        request       = Net::HTTP::Post.new(uri.path, initheader = {'Content-Type' =>'application/json'})
        json_data = JSON.parse(args[:data],:symbolize_names => true)
        json_data[:documents][0][:copies] = 1
        json_data[:documents][0][:report_file] = control[0][:report_file]
        json_data[:documents][0][:auto_printable] = false
        json_data[:documents][0][:data][:attributes].delete(:certificate)
        request.body  = json_data.to_json
        http_response = http.request(request)
        if http_response.code == "200"
          %x[open #{http_response.body}]
        else
          ap JSON.parse(http_response.body)
        end
      end
    end

    def self.initsee (a_pg_conn, a_recreate = false)
      if a_recreate
        a_pg_conn.exec(%Q[

          DROP FUNCTION IF EXISTS see (
            a_module          text,
            a_version         text,
            a_query_map       text,
            a_calc_parameters text
          );

          DROP FUNCTION IF EXISTS see (
            a_module          text,
            a_version         text,
            a_query_map       text,
            a_calc_parameters text,
            a_log             text,
            a_debug           boolean
          );

          DROP FUNCTION IF EXISTS see_payroll (
            a_module          text,
            a_version         text,
            a_query_map       text,
            a_calc_parameters text,
            a_clones          text,
            a_log             text,
            a_debug           boolean
          );

          DROP FUNCTION IF EXISTS see_evaluate_expression (
            a_expression text
          );
          DROP TYPE IF EXISTS see_record;
          DROP TABLE IF EXISTS pg_see_json_table;
        ]);
      end
      begin
        a_pg_conn.exec(%Q[
          CREATE TYPE see_record AS (json text, status text);
        ])
      rescue Exception => e
        if a_recreate
          raise e
        end
      end
      begin
        a_pg_conn.exec(%Q[
          CREATE OR REPLACE FUNCTION see (
            a_module          text,
            a_version         text,
            a_query_map       text,
            a_calc_parameters text,
            a_log             text default null,
            a_debug           boolean default false
          ) RETURNS see_record AS '$libdir/pg-see.so', 'see' LANGUAGE C STRICT;

          CREATE OR REPLACE FUNCTION see_payroll (
            a_module          text,
            a_version         text,
            a_query_map       text,
            a_calc_parameters text,
            a_clones          text,
            a_log             text default null,
            a_debug           boolean default false
          ) RETURNS see_record AS '$libdir/pg-see.so', 'see_payroll' LANGUAGE C STRICT;

          CREATE OR REPLACE FUNCTION see_evaluate_expression (
            a_expression text
          ) RETURNS see_record AS '$libdir/pg-see.so', 'see_evaluate_expression' LANGUAGE C STRICT;

          ])
      rescue Exception => e
        if a_recreate
          raise e
        end
      end
      begin
        a_pg_conn.exec(%Q[
           CREATE TABLE pg_see_json_table (
             namespace   character varying(255),
             table_name  character varying(255),
             version     character varying(20),
             is_model    boolean NOT NULL DEFAULT FALSE,
             json        text,
             commit_hash character varying(255),
             CONSTRAINT pg_see_json_table_pkey PRIMARY KEY(namespace, table_name, version)
           );
        ])
      rescue => e
        if a_recreate
          raise e
        end
      end

    end
  end
end

def _log(message, prefix = nil)
  message = message.is_a?(String) ? message : message.inspect
  prefix = "SP::Duh#{prefix.blank? ? '' : ' [' + prefix + ']'}: "
  if Rails.logger && !defined?(Rails::Console)
    Rails.logger.debug "#{prefix}#{message}"
  else
    puts "#{prefix}#{message}"
  end
end

# Configure the I18n module for correct usage when outside a Rails app (tests)
I18n.load_path += Dir[File.join(SP::Duh.root, 'config', 'locales', '*.{rb,yml}')]
I18n.default_locale = :pt
