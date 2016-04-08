module SP
  module Duh
    module JSONAPI
      module Exceptions

        # JSONAPI service and configuration errors

        class ServiceSetupError < SP::Duh::Exceptions::GenericError ; ; end
        class ServiceProtocolError < SP::Duh::Exceptions::GenericDetailedError ; ; end
        class InvalidResourceConfigurationError < SP::Duh::Exceptions::GenericDetailedError ; ; end
        class DuplicateResourceError < SP::Duh::Exceptions::GenericDetailedError ; ; end
        class SaveConfigurationError < SP::Duh::Exceptions::GenericError ; ; end

        # JSONAPI model querying errors

        class GenericModelError < SP::Duh::Exceptions::GenericError

          attr_reader :status

          def initialize(status, message = nil, nested = $!)
            @status = status || 403
            super(message, nested)
          end
        end

      end
    end
  end
end