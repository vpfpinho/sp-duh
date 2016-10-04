module SP
  module Duh
    module Adapters
      module PG
        module TextDecoder
          class Json < ::PG::SimpleDecoder
            def decode(string, tuple=nil, field=nil)
              JSON.parse string
            end
          end
        end
      end
    end
  end
end