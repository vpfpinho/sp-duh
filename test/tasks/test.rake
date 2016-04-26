namespace :test do

  namespace :jsonapi do
    desc "Start the JSONAPI test server"
    task :server do
      system "ruby test/jsonapi/server.rb"
    end
  end

end