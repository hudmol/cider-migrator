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
