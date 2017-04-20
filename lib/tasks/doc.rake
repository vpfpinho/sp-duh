namespace :sp do
  namespace :duh do
    namespace :jsonapi do
      namespace :doc do

        desc "Generate (JSONAPI) API documentation"
        task :generate, [ :publisher, :version, :folder ] => :environment do |task, arguments|

          if arguments[:publisher].nil? || arguments[:version].nil?
            raise "Usage: rake sp:duh:jsonapi:doc:generate[<resource publisher>,<API version>[,<documentation_folder; default = ./apidoc>]"
          end

          Rails.logger = Logger.new(STDOUT)

          generator = SP::Duh::JSONAPI::Doc::Generator.new(ActiveRecord::Base.connection.raw_connection)
          if arguments[:folder].nil?
            generator.generate(arguments[:publisher], arguments[:version])
          else
            generator.generate(arguments[:publisher], arguments[:version], arguments[:folder])
          end

        end

      end
    end
  end
end