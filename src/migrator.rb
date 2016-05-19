require_relative 'migration_store'
require_relative 'promise_store'
require_relative 'tree_store'
require_relative 'log'

require 'jsonmodel'


def show_usage
  raise "Usage: #{$0} <CIDER URL> <ArchivesSpace backend URL> <ArchivesSpace repo id> <ArchivesSpace admin password>"
end

$cider_url = ARGV.fetch(0) { show_usage }
$backend_url = ARGV.fetch(1) { show_usage }
$repo_id = ARGV.fetch(2) { show_usage }
$admin_password = ARGV.fetch(3) { show_usage }

$basedir = File.expand_path(File.join(File.dirname(__FILE__), ".."))

# # Load all converters
# require_relative 'converters/base_converter'
#
# Dir.glob(File.join($basedir, "src/converters/*.rb")).sort.each do |file|
#   require File.absolute_path(file)
# end


class ArchivesSpaceConnection

  class PermissiveValidator
    def method_missing(*)
      true
    end
  end

  def self.setup(admin_password)
    JSONModel::init(:client_mode => true,
                    :url => $backend_url,
                    :enum_source => PermissiveValidator.new)

    login!("admin", admin_password)
  end


  def self.login!(username, password)
    uri = JSONModel(:user).uri_for("#{username}/login?expiring=false")

    response = JSONModel::HTTP.post_form(uri, 'password' => password)

    if response.code == '200'
      Thread.current[:backend_session] = JSON.parse(response.body)['session']
    else
      raise "ArchivesSpace Login failed: #{response.body}"
    end
  end

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

  def initialize(out_fh)
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

      chatty("Doing something clever", store, tree_store) do
      end

      store.all_records(:resolve_promises_opts => {:discard_failed_promises => true}) do |record|
        @out.puts record.to_json
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

class ArchivesSpaceImport

  def initialize(repo_id)
    @repo_id = repo_id
  end


  def batch_import(file)
    JSONModel::HTTP.post_json_file(URI.join(JSONModel::HTTP.backend_url, "/repositories/#{@repo_id}/batch_imports"),
                                   file) do |response|
      response.read_body do |chunk|
        Log.info(chunk)
      end
    end
  end

end


def main
  ArchivesSpaceConnection.setup($admin_password)

  aspace = ArchivesSpaceImport.new($repo_id)

  Log.info("Converting records...")

  exported_file = File.join($basedir, "exported_#{Time.now.to_i}.json")

  File.open(exported_file, "w") do |fh|
    Migrator.new(fh).call
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
