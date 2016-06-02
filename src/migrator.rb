require_relative 'archivesspace_import'
require_relative 'archivesspace_connection'
require_relative 'migration_store'
require_relative 'promise_store'
require_relative 'tree_store'
require_relative 'log'

require 'jsonmodel'
require 'sequel'


def show_usage
  raise "Usage: #{$0} <CIDER URL> <ArchivesSpace backend URL> <ArchivesSpace repo id> <ArchivesSpace admin password>"
end

$cider_url = ARGV.fetch(0) { show_usage }
$backend_url = ARGV.fetch(1) { show_usage }
$repo_id = ARGV.fetch(2) { show_usage }
$admin_password = ARGV.fetch(3) { show_usage }

$basedir = File.expand_path(File.join(File.dirname(__FILE__), ".."))

# Load all converters
require_relative 'converters/converter'

Dir.glob(File.join($basedir, "src/converters/*.rb")).sort.each do |file|
  require File.absolute_path(file)
end


include JSONModel


class TempDir

  def self.mktmpdir
    if ENV['MIGRATE_TMP_DIR']
      custom_tmp_dir = ENV['MIGRATE_TMP_DIR']
      FileUtils.mkdir_p(custom_tmp_dir)
      yield(custom_tmp_dir)
    else
      Dir.mktmpdir do |dir|
        yield(dir)
      end
    end
  end

end


class Migrator

  def self.promise(type, id)
    {'_promise' => {'type' => type, 'id' => id}}
  end

  def initialize(cider_db, out_fh)
    @cider_db = cider_db
    @out = out_fh
  end

  def call
    TempDir.mktmpdir do |dir|
      store_dir = File.join(dir, "migration")

      if Dir.exists?(store_dir)
        # Move it away to avoid conflicts.
        FileUtils.mv(store_dir, "#{store_dir}-#{SecureRandom.hex}")
      end

      FileUtils.mkdir_p(store_dir)

      store = MigrationStore.new(store_dir)
      tree_store = TreeStore.new(store)

      chatty("Extracting Agent records from CIDER RCRs", store, tree_store) do
        AgentConverter.new(@cider_db).call(store)
      end

      chatty("Extracting Subject records from various CIDER tables", store, tree_store) do
        SubjectConverter.new(@cider_db).call(store)
      end

      chatty("Extracting Resource records from CIDER Collections", store, tree_store) do
        ResourceConverter.new(@cider_db).call(store)
      end

      chatty("Extracting ArchivalObject records from CIDER Objects", store, tree_store) do
        ArchivalObjectConverter.new(@cider_db).call(store, tree_store)
      end

      chatty("Extracting Digital Object records from CIDER Digital Objects", store, tree_store) do
        DigitalObjectConverter.new(@cider_db).call(store)
      end

      chatty("Resolving all parent/child relationships", store, tree_store) do
        # Parent/child & collection relationships
        tree_store.deliver_all_promises!
      end

      chatty("Storing records", store, tree_store) do
        store.all_records(:resolve_promises_opts => {:discard_failed_promises => false}) do |record|
          @out.puts record.to_json
        end
      end
    end
  end

  private

  def chatty(description, store, tree_store)
    Log.info(description)
    yield

    $stderr.puts("Bytes in tree_store: #{tree_store.byte_size}")

    Log.info("Finished: #{description}")
  end

end


def main
  ArchivesSpaceConnection.setup($admin_password)

  aspace = ArchivesSpaceImport.new($repo_id)

  Log.info("Converting records...")

  exported_file = File.join($basedir, "exported_#{Time.now.to_i}.json")

  Sequel.connect($cider_url) do |db|
    File.open(exported_file, "w") do |fh|
      Migrator.new(db, fh).call
    end
  end

  Log.info("Validating")

  validated_file = File.join($basedir, "validated_#{Time.now.to_i}.json")

  File.open(exported_file, "r") do |input|
    File.open(validated_file, "w") do |out|
      out.puts("[")

      while (json = input.gets) do
        record = JSON.parse(json)
        record_type = record.fetch('jsonmodel_type').intern

        begin
          JSONModel(record_type).from_hash(record)

          out.puts(json)

          if input.eof?
            out.puts "]"
          else
            out.puts ","
          end
        rescue JSONModel::ValidationException => e
          Log.warn("Rejected record!")
          Log.warn("Error was: #{e}")
          Log.warn("Record JSON: #{json}")
        end
      end
    end
  end


  Log.info("Sending records to ArchivesSpace")

  aspace.batch_import(validated_file)
end


main
