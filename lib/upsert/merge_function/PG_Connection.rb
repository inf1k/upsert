require 'upsert/merge_function/postgresql'

class Upsert
  class MergeFunction
    # @private
    class PG_Connection < MergeFunction
      include Postgresql

      def execute(row)
        first_try = true
        values = []
        values += row.selector.values.select {|v| !v.is_a?(Upsert::RawSql) }
        values += row.setter.values.select {|v| !v.is_a?(Upsert::RawSql) }

        hstore_delete_handlers.each do |hstore_delete_handler|
          values << row.hstore_delete_keys.fetch(hstore_delete_handler.name, [])
        end
        Upsert.logger.debug do
          %{[upsert]\n\tSelector: #{row.selector.inspect}\n\tSetter: #{row.setter.inspect}}
        end

        Upsert.logger.debug do
          sql
        end

        begin
          bound_values = values.select { |v| !v.is_a?(Upsert::RawSql) }.map { |v| connection.bind_value(v) }

          Upsert.logger.debug do
            bound_values
          end

          connection.execute sql_for(row), bound_values
        rescue PG::Error => pg_error
          if pg_error.message =~ /function #{name}.* does not exist/i
            if first_try
              Upsert.logger.info %{[upsert] Function #{name.inspect} went missing, trying to recreate}
              first_try = false
              create!
              retry
            else
              Upsert.logger.info %{[upsert] Failed to create function #{name.inspect} for some reason}
              raise pg_error
            end
          else
            raise pg_error
          end
        end
      end

      # strangely ? can't be used as a placeholder
      def sql
        @sql ||= begin
          bind_params = []
          i = 1
          # (selector_keys.length + setter_keys.length).times do
          #   bind_params << "$#{i}"
          #   i += 1
          # end

          selector.each do |name, value|
            if value.is_a?(Upsert::RawSql)
              bind_params << value.to_sql
            else
              bind_params << "$#{i}"
              i += 1
            end
          end

          setter.each do |name, value|
            if value.is_a?(Upsert::RawSql)
              bind_params << value.to_sql
            else
              bind_params << "$#{i}"
              i += 1
            end
          end

          hstore_delete_handlers.length.times do
            bind_params << "$#{i}::text[]"
            i += 1
          end
          %{SELECT #{name}(#{bind_params.join(', ')})}
        end
      end

      # strangely ? can't be used as a placeholder
      def sql_for(row)
        bind_params = []
        i = 1
        # (selector_keys.length + setter_keys.length).times do
        #   bind_params << "$#{i}"
        #   i += 1
        # end

        row.selector.each do |name, value|
          if value.is_a?(Upsert::RawSql)
            bind_params << value.to_sql
          else
            bind_params << "$#{i}"
            i += 1
          end
        end

        row.setter.each do |name, value|
          if value.is_a?(Upsert::RawSql)
            bind_params << value.to_sql
          else
            bind_params << "$#{i}"
            i += 1
          end
        end

        hstore_delete_handlers.length.times do
          bind_params << "$#{i}::text[]"
          i += 1
        end
        %{SELECT #{name}(#{bind_params.join(', ')})}
      end
      # end


    end
  end
end
