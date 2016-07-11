class StaffConverter < Converter
  def call(store)
    Log.info("Going to process #{db[:staff].count} staff records")

    db[:staff].each do |staff|
      store.put_agent_person(build_agent(staff), 'staff')
    end
  end

  private

  def build_agent(staff)
    {
      'id' => "#{staff[:id]}",
      'jsonmodel_type' => 'agent_person',
      'agent_type' => 'agent_person',
      'names' => [build_name(staff)],
    }
  end

  def build_name(staff)
    {
      'sort_name_auto_generate' => true,
      'jsonmodel_type' => 'name_person',
      'primary_name' => staff[:last_name],
      'rest_of_name' => staff[:first_name],
      'name_order' => 'inverted',
      'source' => 'cider',
    }
  end
end
