require 'csv'

class Users

  def initialize(file)
    @csv = CSV.open(file, :headers => true)
  end

  def import
    while (entry = @csv.readline)
      username = entry['Username']

      group_uri = nil
      group_code = nil
      is_admin = false

      if entry['Permission group'] == 'Repository manager'
        group_code ='repository-managers'
      elsif entry['Permission group'] == 'System administrator'
        group_code = 'repository-managers'
        is_admin = true
      elsif entry['Permission group'] == 'Archivist'
        group_code = 'repository-archivists'
      end

      group_uri = group_uri_for(group_code) if group_code

      if group_uri.nil?
        Log.warn("No matching group for #{username} and the Permission group #{entry['Permission group']}")
      end

      begin
        user = JSONModel(:user).from_hash({
                                            'username' => username,
                                            'name' => "#{entry['First Name']} #{entry['Last Name']}",
                                            'first_name' => entry['First Name'],
                                            'last_name' => entry['Last Name'],
                                            'is_admin' => is_admin,
                                            'groups' => [group_uri].compact
                                          })

        password = "#{entry['Last Name'].downcase}123!"
        user.save(:password => password)
        user.refetch

        Log.info("Created user account for username '#{username}' with password '#{password}'")
        Log.info("Attached #{username} to group #{group_code} (#{group_uri})") if group_uri
        Log.info("Assigned #{username} as a system administrator") if is_admin
      rescue
        Log.warn("Error creating user #{username}")
        Log.warn($!)
      end
    end
  end

  def close
    @csv.close if @csv
    @csv = nil
  end

  private

  def group_uri_for(group_code)
    groups.each do |group|
      if group['group_code'] == group_code
        return group['uri']
      end
    end

    raise "ArchivesSpace Group for #{group_code} not found"
  end


  def groups
    return @groups if @groups

    @groups = JSONModel::HTTP.get_json("/repositories/#{$repo_id}/groups")

    @groups
  end
end