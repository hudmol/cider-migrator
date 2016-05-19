class AgentGroup

  def initialize(store)
    @migration_store = store
    @stored_agents_by_sort_name = {}
    @known_duplicates = {}
  end


  def inspect
    "#<AgentGroup>"
  end


  def record_agent(record, store)
    @stored_agents_by_sort_name[sort_name(record)] = [record['id'], store]
  end


  def find_matching_agent(record)
    (id, store) = @stored_agents_by_sort_name.fetch(sort_name(record)) {
      return nil
    }

    store[id]
  end


  def group(opts)
    agent_key = opts.fetch(:role) + "_" + opts.fetch(:id)
    @known_duplicates[agent_key] = "AgentGroup_#{opts.fetch(:agent_group)}"
  end


  def known_duplicate?(record, role)
    @known_duplicates.has_key?("#{role}_#{record['id']}")
  end


  def group_identifier(record, role)
    @known_duplicates.fetch("#{role}_#{record['id']}")
  end


  def needs_merge?(record, role)
    store = @migration_store.find_store_containing_record(group_identifier(record, role))
    !store.nil?
  end


  def merge_agent(record, role)
    agent_group_id = @known_duplicates.fetch("#{role}_#{record['id']}")
    store = @migration_store.find_store_containing_record(agent_group_id)

    other_agent = store[agent_group_id]

    merged = deep_merge(other_agent, record)

    merged['uri'] = other_agent['uri']
    merged['id'] = agent_group_id

    store[agent_group_id] = merged

    other_agent['uri']
  end


  private

  def sort_name(record)
    record['names'][0].fetch('sort_name')
  end


  def deep_merge(obj1, obj2)

    if [obj1, obj2].include?(nil)
      return [obj1, obj2].compact.first.clone
    end

    raise "Can't merge objects of different types" if obj1.class != obj2.class

    if obj1.is_a?(Hash)
      Hash[(obj1.keys + obj2.keys).uniq.map {|k|
             [k, deep_merge(obj1[k], obj2[k])]
           }]
    elsif obj1.is_a?(Array)
      # Dedupe based on record keys that aren't internal to our migrator
      (obj1 + obj2).uniq {|obj|
        if obj.is_a?(Hash)
          obj.select {|k| !k.is_a?(String) || !k.start_with?("_")}
        else
          obj
        end
      }
    else
      if obj1 != obj2
        Log.info("Collision between #{obj1} and #{obj2}.  Taking #{obj1}")
      end

      obj1
    end
  end
end
