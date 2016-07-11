class StaffConverter < Converter
  def call(store)
    Log.info("Going to process #{db[:staff].count} staff records")

    @seen_names = {}

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

  def uniqify(first_name, last_name)
    key = [first_name, last_name]

    result = key

    if @seen_names[key]
      result = [
        first_name,
        last_name + '_' + @seen_names[key].to_s
      ]
      @seen_names[key] += 1
    else
      @seen_names[key] = 2
    end

    result
  end

  def build_name(staff)
    first_name, last_name = uniqify(staff[:first_name], staff[:last_name])

    {
      'sort_name_auto_generate' => true,
      'jsonmodel_type' => 'name_person',
      'primary_name' => last_name,
      'rest_of_name' => first_name,
      'name_order' => 'inverted',
      'source' => 'cider',
    }
  end
end
