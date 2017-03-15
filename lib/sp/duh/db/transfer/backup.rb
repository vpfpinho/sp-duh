module SP
  module Duh
    module Db
      module Transfer

        def self.log_with_time(message)
          puts "[#{Time.now.strftime('%d-%m-%Y %H:%M')}] #{message}"
        end

        class Backup

          attr_reader :status

          def initialize(source_pg_connection)
            @connection = source_pg_connection
          end

          def execute(company_id, dump_file = nil)
            self.status = :ok
            @company_id = company_id
            yield(:before_execute) if block_given?
            before_execute
            yield(:do_execute) if block_given?
            do_execute(dump_file) if self.status == :ok
            yield(:after_execute) if block_given?
            after_execute if self.status == :ok
          end

          protected

            def before_execute
              SP::Duh::Db::Transfer.log_with_time "STARTED backing up company #{@company_id}"
              SP::Duh::Db::Transfer.log_with_time "Preparing backup..."
              @started_at = Time.now
              @schemas = @connection.exec %Q[
                SELECT * FROM transfer.backup_before_execute(#{@company_id});
              ]
              @schemas = @schemas.map { |result| result['schema_name'] }
            end

            def do_execute(dump_file = nil)
              SP::Duh::Db::Transfer.log_with_time "Executing backup..."
              dump_file = "#{Time.now.strftime('%Y%m%d%H%M')}_c#{@company_id}.dump"  if dump_file.nil?
              command = "pg_dump -Fc -O --quote-all-identifiers --data-only -n #{@schemas.join(' -n ')} -h #{@connection.host} -p #{@connection.port} -U #{@connection.user} #{@connection.db} > #{dump_file}"
              SP::Duh::Db::Transfer.log_with_time command
              %x[ #{command} ]
              if $?.exitstatus != 0
                File.delete dump_file
                self.status = :error_dumping_company
              end
            end

            def after_execute
              @ended_at = Time.now
              SP::Duh::Db::Transfer.log_with_time "FINISHED backing up company #{@company_id} in #{(@ended_at - @started_at).round(2)}s"
            end

            def status=(value)
              @status = value
              if value != :ok
                @ended_at = Time.now
                SP::Duh::Db::Transfer.log_with_time "CANCELED backing up company #{@company_id} in #{(@ended_at - @started_at).round(2)}s"
              end
            end

        end

      end
    end
  end
end