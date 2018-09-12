#
# Copyright (c) 2011-2017 Cloudware S.A. All rights reserved.
#
# This file is part of sp-duh.
#
# sp-duh is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sp-duh is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with sp-duh.  If not, see <http://www.gnu.org/licenses/>.
#
# encoding: utf-8
#
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'version'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'engine'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'repl'))

# JSONAPI library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'resource_publisher'))
# Parameters class
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'parameters'))
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

# API documentationlibrary classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'doc', 'generator'))

# i18n classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'i18n', 'excel_loader'))

# SP helper lib
require 'sp-excel-loader'

module SP
  module Duh
    def self.root
      File.expand_path '../../..', __FILE__
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
