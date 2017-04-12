module SP
  module Duh
    module Db
      module Transfer

        class Restore

          def initialize(destination_pg_connection)
            @connection = destination_pg_connection
          end

          def execute(company_id, dump_file, skip = false)

            @company_id = company_id
            @dump_file = dump_file

            yield(:before, :before_execute) if block_given?
            results = before_execute(skip)
            yield(:after, :before_execute, results) if block_given?

            return results if skip

            yield(:before, :do_execute) if block_given?
            results = do_execute
            yield(:after, :do_execute, results) if block_given?

            yield(:before, :after_execute) if block_given?
            results = after_execute
            yield(:after, :after_execute, results) if block_given?

            return results

          end

          protected

            def before_execute(skip = false)
              SP::Duh::Db::Transfer.log_with_time "STARTED restoring company #{@company_id} from dump #{@dump_file}"
              SP::Duh::Db::Transfer.log_with_time "Preparing restore..."
              @started_at = Time.now
              meta_schema = @connection.exec %Q[
                SELECT * FROM transfer.restore_before_before_execute(#{@company_id});
              ]
              meta_schema = meta_schema.first.values.first
              command = "pg_restore -Fc -n #{meta_schema} --data-only -h #{@connection.host} -p #{@connection.port} -U #{@connection.user} -d #{@connection.db} < #{@dump_file}"
              SP::Duh::Db::Transfer.log_with_time "Restoring the backup metadata..."
              SP::Duh::Db::Transfer.log_with_time command
              result = %x[ #{command} ]
              if skip
                SP::Duh::Db::Transfer.log_with_time "Processing metadata..."
              else
                SP::Duh::Db::Transfer.log_with_time "Processing metadata and foreign records..."
              end
              @schemas = @connection.exec %Q[
                SELECT * FROM transfer.restore_after_before_execute(#{@company_id}, #{skip});
              ]
              @schemas = @schemas.map { |result| result['schema_name'] }
            end

            def do_execute
              SP::Duh::Db::Transfer.log_with_time "Executing restore..."
              command = "pg_restore -Fc -n #{@schemas.join(' -n ')} -h #{@connection.host} -p #{@connection.port} -U #{@connection.user} -d #{@connection.db} < #{@dump_file}"
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