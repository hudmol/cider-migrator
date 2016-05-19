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
