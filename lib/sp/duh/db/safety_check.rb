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
require 'base64'

module SP
  module Duh
    module Db

      #
      # Call this to check if the database is not a production database where it's
      # dangerous to make development stuff. It checks the presence of a magic parameter
      # on the PG configuration that marks the database as a development arena
      #
      # Returns true if the DB is marked safe for development, other wise raises the exception
      #
      # @param connection a PG connection 
      #
      def self.safety_check (connection)
        begin
          port   = connection.exec('SHOW port').first['port']
          listen = connection.exec('SHOW listen_addresses').first['listen_addresses']
          dbname = connection.exec('SELECT current_database()').first['current_database']
          flag   = connection.exec("SHOW casper.db_safety_#{dbname}").first["casper.db_safety_#{dbname}"]
          if Base64.strict_encode64("#{listen}:#{port}:#{dbname}").strip == flag
            return true
          end        
        rescue Exception => e
          # Fall through
        end
        raise ::SP::Duh::Exceptions::GenericError.new('For safety reasons this task can\'t be run on this database, no you don\'t know what you are doing')
      end

    end
  end
end