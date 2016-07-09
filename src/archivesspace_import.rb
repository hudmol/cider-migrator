class ArchivesSpaceImport

  def initialize(repo_id)
    @repo_id = repo_id
  end

  def create_repo(code, description)
    unless JSONModel::HTTP.get_json("/repositories/#{@repo_id}")
      Log.info("Creating repo...")

      JSONModel::HTTP.post_json(URI.join(JSONModel::HTTP.backend_url, "/repositories"),
                                {:repo_code => code,
                                 :name => description}.to_json) do |response|
        Log.info(response.body)
      end
    end
  end

  def batch_import(file)
    JSONModel::HTTP.post_json_file(URI.join(JSONModel::HTTP.backend_url, "/repositories/#{@repo_id}/batch_imports?skip_results=true"),
                                   file) do |response|
      response.read_body do |chunk|
        Log.info(chunk)
      end
    end
  end

end
