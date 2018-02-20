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
    module Exceptions

      # Generic errors

      class GenericError < StandardError

        attr_reader :nested
        attr_reader :raw_backtrace

        def initialize(message = nil, nested = $!)
          if message.nil?
            message = ::I18n.t("sp-duh.exceptions.#{type.underscore.gsub('/','.')}") if I18n.exists?("sp-duh.exceptions.#{type.underscore.gsub('/','.')}")
          end
          super(message)
          @nested = nested
        end

        def set_backtrace(backtrace)
          @raw_backtrace = backtrace
          if nested
            backtrace = backtrace - nested_raw_backtrace
            backtrace += ["#{nested.backtrace.first}: #{nested.message} (#{nested.class.name})"]
            backtrace += nested.backtrace[1..-1] || []
          end
          super(backtrace)
        end

        protected

          def type ; self.class.name.sub("SP::Duh::", "").sub("Exceptions::", "") ; end

        private

          def nested_raw_backtrace
            nested.respond_to?(:raw_backtrace) ? nested.raw_backtrace : nested.backtrace
          end
      end

      class GenericDetailedError < GenericError
        def initialize(details = {})
          message = ::I18n.t("sp-duh.exceptions.#{type.underscore.gsub('/','.')}", details)
          super(message)
        end
      end

    end
  end
end