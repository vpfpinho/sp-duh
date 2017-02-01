module SP
  module Duh
    module Db
      module Transfer

        class Restore

          def initialize(destination_pg_connection)
            @connection = destination_pg_connection
          end

          def execute(company_id, dump_file)
            @company_id = company_id
            @dump_file = dump_file
            yield(:before_execute) if block_given?
            before_execute
            yield(:do_execute) if block_given?
            do_execute
            yield(:after_execute) if block_given?
            after_execute
          end

          protected

            def before_execute
              SP::Duh::Db::Transfer.log_with_time "STARTED restoring company #{@company_id} from dump #{@dump_file}"
              SP::Duh::Db::Transfer.log_with_time "Preparing restore..."
              @started_at = Time.now
              @connection.exec %Q[
                DROP SCHEMA IF EXISTS _meta_ CASCADE;
                CREATE SCHEMA _meta_;
              ]
              command = "pg_restore -Fc -n _meta_ -U #{@connection.user} -d #{@connection.db} < #{@dump_file}"
              SP::Duh::Db::Transfer.log_with_time "Restoring the backup metadata..."
              SP::Duh::Db::Transfer.log_with_time command
              result = %x[ #{command} ]
              SP::Duh::Db::Transfer.log_with_time "Processing metadata and foreign records..."
              @schemas = @connection.exec %Q[
                SELECT * FROM transfer.restore_before_execute(#{@company_id});
              ]
              @schemas = @schemas.map { |result| result['schema_name'] }
            end

            def do_execute
              SP::Duh::Db::Transfer.log_with_time "Executing restore..."
              command = "pg_restore -Fc -n #{@schemas.join(' -n ')} -U #{@connection.user} -d #{@connection.db} < #{@dump_file}"
              SP::Duh::Db::Transfer.log_with_time command
              result = %x[ #{command} ]
            end

            def after_execute
              SP::Duh::Db::Transfer.log_with_time "Finishing restore..."
              @connection.exec %Q[
                SELECT * FROM transfer.restore_after_execute(#{@company_id});
              ]
              @ended_at = Time.now
              SP::Duh::Db::Transfer.log_with_time "FINISHED restoring company #{@company_id} in #{(@ended_at - @started_at).round(2)}s"
            end

        end

      end
    end
  end
end