module SP
  module Duh
    module Adapters
      module PG
        module TextEncoder
          class Json < ::PG::SimpleEncoder
            def decode(string, tuple=nil, field=nil)
              JSON.parse string
            end
          end
        end
      end
    end
  end
end