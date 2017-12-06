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
module SP
  module Duh
    module Migrations
      extend ::ActiveSupport::Concern

      included do
      end

      module ClassMethods

        attr_reader :migrations_root

        def migrator(pg_connection)
          if @migrator.nil? || @migrator.root != migrations_root || @migrator.connection != pg_connection
            @migrator = Migrator.new(pg_connection, migrations_root)
          end
          @migrator
        end

        private

          attr_writer :migrations_root
      end

    end
  end
end