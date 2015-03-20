class Upsert
  class RawSql

    def initialize(sql)
      @sql = sql
    end

    def to_sql
      @sql
    end

  end
end
