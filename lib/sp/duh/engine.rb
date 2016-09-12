require 'rails'

module SP
  module Duh
    class Engine < ::Rails::Engine
      isolate_namespace SP::Duh

      initializer :append_migrations do |app|
        unless app.root.to_s.match root.to_s
          app.config.paths["db/migrate"] += config.paths["db/migrate"].expanded
        end
      end
    end
  end
end
