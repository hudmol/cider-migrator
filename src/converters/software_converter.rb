class SoftwareConverter < Converter

  def call(store)
    Log.info("Going to process #{db[:application].count} application records")

    db[:application].each do |application|
      store.put_agent_software(build_from_application(application), 'application')
    end
  end

  private

  def build_from_application(application)
    {
      'id' => "#{application[:id]}",
      'jsonmodel_type' => 'agent_software',
      'agent_type' => 'agent_software',
      'names' => [{
        'sort_name_auto_generate' => true,
        'jsonmodel_type' => 'name_software',
        'software_name' => application[:function] + " - " + application[:name],
        'source' => 'cider',
      }],
      'notes' => [{
        'jsonmodel_type' => 'note_bioghist',
        'publish' => true,
        'label' => 'Function',
        'subnotes' => [{
          'jsonmodel_type' => 'note_text',
          'content' => application[:function],
        }]
      }]
    }
  end
end
