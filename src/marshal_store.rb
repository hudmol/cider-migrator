require 'fileutils'
require 'rufus-lru'
require 'set'

class RecordNotFound < StandardError
end

class MarshalStore

  def initialize(dir, buffer_size = 5, cache_size = 10)
    @stored_ids = Set.new
    @storage_dir = dir
    @buffer_size = buffer_size
    @cache_size = cache_size

    raise "Invalid sizings" if @cache_size < @buffer_size

    @dirty_records = Set.new

    FileUtils.mkdir_p(@storage_dir)

    clear!
  end

  def []=(key, value)
    @memory_store[key] = value

    @dirty_records << key
    @stored_ids << key

    if @dirty_records.length > @buffer_size
      commit
    end

    value
  end

  def each
    @stored_ids.each do |id|
      yield self[id]
    end
  end


  def has_key?(key)
    @memory_store.has_key?(key) || File.exists?(File.join(@storage_dir, key.to_s))
  end


  def [](key)
    ensure_loaded!(key)

    @memory_store.fetch(key)
  end

  def commit
    @dirty_records.each do |key|
      value = @memory_store[key]
      File.write(File.join(@storage_dir, key.to_s), Marshal.dump(value))
    end

    @dirty_records.clear
  end

  def clear!
    @memory_store = Rufus::Lru::Hash.new(@cache_size)
  end

  private

  def ensure_loaded!(key)
    if !@memory_store.has_key?(key)
      commit
      begin
        @memory_store[key] = Marshal.load(File.read(File.join(@storage_dir, key.to_s)))
      rescue
        raise RecordNotFound.new(key)
      end
    end
  end

end
