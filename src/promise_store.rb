class PromiseStore

  def initialize(target_file)
    java.lang.Class.forName("org.sqlite.JDBC")

    @db = java.sql.DriverManager.getConnection("jdbc:sqlite:#{target_file}")

    init_db!
  end


  def has_promise?(foreign_key, id)
    !fetch_promise(foreign_key, id).nil?
  end


  def deliver_promise(foreign_key, id, value)
    if has_promise?(foreign_key, id)
      Log.warn("Already delivered promise for #{foreign_key}, #{id}, #{value}.  Skipped")
      return nil
    end

    if update("insert into promises (foreign_key, id, value) values (?, ?, ?)", foreign_key, id, value) == 0
      raise "Failed when delivering promise #{foreign_key}, #{id}, #{value}"
    end

    true
  end


  def fetch_promise(foreign_key, id)
    with_results("select value from promises where foreign_key = ? AND id = ?", foreign_key, id) do |result|
      return result.get_string(1)
    end
  end


  private


  def prepare(sql, params)
    statement = @db.prepareStatement(sql)

    params.each_with_index do |param, idx|
      statement.set_string(idx + 1, param)
    end

    statement
  end


  def with_results(sql, *params)
    statement = nil
    results = nil

    begin
      statement = prepare(sql, params)
      results = statement.executeQuery

      while results.next
        yield(results)
      end
    ensure
      statement.close if statement
      results.close if results
    end
  end


  def update(sql, *params)
    do_update(sql, params, :update)
  end

  def execute(sql, *params)
    do_update(sql, params, :execute)
  end


  def do_update(sql, params, mode = :update)
    statement = @db.prepareStatement(sql)

    begin
      params.each_with_index do |param, idx|
        statement.set_string(idx + 1, param)
      end

      if mode == :update
        statement.executeUpdate
      else
        statement.execute
      end
    ensure
      statement.close
    end
  end



  def init_db!
    update("create table if not exists promises (foreign_key string, id string, value string)")
    update("create index if not exists idx_1 on promises (foreign_key, id, value)")

    update("PRAGMA synchronous = OFF")
    execute("PRAGMA journal_mode = OFF")

  end

end
