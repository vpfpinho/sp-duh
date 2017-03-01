namespace :sp do
  namespace :duh do
    namespace :jsonapi do

      desc "Reload all JSONAPI resources from all configured publishers"
      task :reload => :environment do
        Rails.logger.debug "Reloading JSONAPI configuration from the publishers, for url #{JSONAPI.url}"
        # Use the JSONAPI global settings defined (if not defined, defaults to the standard settings defined in the sp-duh gem)
        JSONAPI.service.configuration.settings = JSONAPI.configuration[:settings]
        # Reload all JSONAPI resources from all configured publishers
        JSONAPI.configuration[:resources][:publishers].each do |publisher|
          JSONAPI.service.configuration.add_publisher(publisher)
        end
        JSONAPI.service.configuration.reload!
      end

    end
  end
end