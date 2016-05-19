require_relative 'agent_groups'
require_relative 'marshal_store'
require 'securerandom'

class MigrationStore

  attr_reader :agent_groups

  def initialize(basedir)
    # @stored_agents_by_sort_name = {}

    @stores = {
      :location => MarshalStore.new(File.join(basedir, "locations")),
      :resource =>  MarshalStore.new(File.join(basedir, "resources")),
      :accession =>  MarshalStore.new(File.join(basedir, "accessions")),
      :archival_object =>  MarshalStore.new(File.join(basedir, "archival_objects")),
      :event =>  MarshalStore.new(File.join(basedir, "events")),
      :agent_person_record_context => MarshalStore.new(File.join(basedir, "agent_person")),
      # :agent_person_creator => MarshalStore.new(File.join(basedir, "agent_creator_people")),
      # :agent_person_acknowledger => MarshalStore.new(File.join(basedir, "agent_acknowledger_people")),
      :agent_corporate_entity_record_context => MarshalStore.new(File.join(basedir, "agent_corporate_entity")),
      # :agent_corporate_entity_contact => MarshalStore.new(File.join(basedir, "agent_corporate_entities_contact")),
      # :agent_corporate_entity_creator => MarshalStore.new(File.join(basedir, "agent_corporate_entities_creator")),
      :agent_family_record_context => MarshalStore.new(File.join(basedir, "agent_family"))
    }

    # @agent_groups = AgentGroup.new(self)

    @promise_store = PromiseStore.new(File.join(basedir, "promises.db"))
  end


  def get_migration_store(type)
    @stores.fetch(type)
  end


  def inspect
    "#<MigrationStore>"
  end


  def find_store_containing_record(id)
    @stores.values.each do |store|
      if store.has_key?(id)
        return store
      end
    end

    return nil
  end


  # def maybe_group_agent(record, role)
  #   if @agent_groups.known_duplicate?(record, role)
  #     if @agent_groups.needs_merge?(record, role)
  #       merged_uri = @agent_groups.merge_agent(record, role)
  #       deliver_promise("#{role}_uri", record['id'], merged_uri)
  #       deliver_promise('contact_relator', record['id'], map_relator(record['_contact_type'])) if role == 'contact'
  # 
  #       return true
  #     else
  #       # Set the ID of this object to our group identifier so we can find it
  #       # for subsequent merges.
  #       record['id'] = @agent_groups.group_identifier(record, role)
  # 
  #       return false
  #     end
  #   else
  #     # Show a warning if this might be a duplicate (based on name) but hasn't
  #     # been marked as such.
  #     if (matching_agent = @agent_groups.find_matching_agent(record))
  #       sort_name = record['names'][0].fetch('sort_name')
  # 
  #       Log.warn("Agent '#{sort_name}' with role #{role} - ID '#{record['id']}' " +
  #                "has the same name as agent with role " +
  #                "'#{matching_agent['_agent_role']}' - ID '#{matching_agent['id']}'")
  #     end
  #   end
  # 
  #   return false
  # end

  def put_agent_person(record, role = 'record_context')
    # Stash and use this because the merge process may replace the record ID
    # with the agent group's ID
    original_id = record['id']

    # grouped = maybe_group_agent(record, role)

    # Handled by merge!
    # return true if grouped

    uri = "/agents/people/import_#{SecureRandom.hex}"

    record['uri'] = uri
    record['_agent_role'] = role

    if put(:"agent_person_#{role}", record)
      store = @stores.fetch(:"agent_person_#{role}")
      # @agent_groups.record_agent(record, store)

      deliver_promise("#{role}_uri", original_id, uri)
      # deliver_promise('contact_relator', original_id, map_relator(record['_contact_type'])) if role == 'contact'

      true
    end
  end


  def put_agent_corporate_entity(record, role = 'record_context')
    # Stash and use this because the merge process may replace the record ID
    # with the agent group's ID
    original_id = record['id']

    # grouped = maybe_group_agent(record, role)
    #
    # # Handled by merge!
    # return true if grouped

    uri = "/agents/corporate_entities/import_#{SecureRandom.hex}"
    record['uri'] = uri
    record['_agent_role'] = role

    store = @stores.fetch(:"agent_corporate_entity_#{role}")
    # @agent_groups.record_agent(record, store)


    if put(:"agent_corporate_entity_#{role}", record)
      deliver_promise("#{role}_uri", original_id, uri)
      # deliver_promise('contact_relator', original_id, map_relator(record['_contact_type'])) if role == 'contact'

      true
    end

  end


  def put_agent_family(record, role = 'record_context')
    # Stash and use this because the merge process may replace the record ID
    # with the agent group's ID
    original_id = record['id']

    # grouped = maybe_group_agent(record, nil)
    # 
    # # Handled by merge!
    # return true if grouped

    uri = "/agents/families/import_#{SecureRandom.hex}"
    record['uri'] = uri
    record['_agent_role'] = nil

    if put(:"agent_family_#{role}", record)
      store = @stores.fetch(:"agent_family_#{role}")
      # @agent_groups.record_agent(record, store)
      deliver_promise("#{role}_uri", original_id, uri)
    end
  end


  def put_location(record)
    uri = "/locations/import_#{SecureRandom.hex}"
    record['uri'] = uri

    if deliver_promise('location_uri', record['id'], uri)
      put(:location, record)
    end
  end


  def put_resource(record)
    uri = "/repositories/#{repo_id}/resources/import_#{SecureRandom.hex}"
    record['uri'] = uri

    if deliver_promise('collection_uri', record['id'], uri)
      put(:resource, record)
    end
  end


  def put_accession(record)
    uri = "/repositories/#{repo_id}/accessions/import_#{SecureRandom.hex}"
    record['uri'] = uri

    if deliver_promise('accession_uri', record['id'], uri)
      put(:accession, record)

      deliver_promise('accession_uri_by_acc_no', record['_acc_no'], uri)
    end

  end


  def put_archival_object(record)
    uri = "/repositories/#{repo_id}/archival_objects/import_#{SecureRandom.hex}"
    record['uri'] = uri

    if deliver_promise('archival_object_uri', record['id'], uri)
      put(:archival_object, record)
    end
  end


  def put_event(record)
    random = SecureRandom.hex
    uri = "/repositories/#{repo_id}/events/import_#{random}"
    record['id'] = random
    record['uri'] = uri

    put(:event, record)
  end


  def all_records(opts = {})
    @stores.keys.each do |record_type|
      each(record_type, opts) do |record|
        yield(record)
      end
    end
  end


  def each(record_type, opts = {})
    @stores.fetch(record_type).each do |record|
      promise_opts = opts.fetch(:resolve_promises_opts, {})
      yield(prune_result({'jsonmodel_type' => record_type.to_s}.merge(resolve_promises(record, promise_opts)),
                         promise_opts))
    end
  end


  def has_promise?(foreign_key, id)
    @promise_store.has_promise?(foreign_key, id)
  end


  def deliver_promise(foreign_key, id, value)
    if has_promise?(foreign_key, id)
      Log.warn("Already delivered promise for #{foreign_key}, #{id}.  Skipped")
      return nil
    end

    @promise_store.deliver_promise(foreign_key, id, value)

    true
  end


  def uri_for(record_type, id)
    get(record_type, id).fetch('uri')
  end

  private


  def map_relator(contact_type)
    # FIXME: need to review these
    relators = {
      'Donor' => 'dnr',
      'Dealer' => 'bsl',
      'Institution' => 'oth'
    }

    if contact_type
      relators.fetch(contact_type)
    end
  end


  def repo_id
    # Why not?
    "3636363636"
  end


  def resolve_promises(record, opts = {})
    if record.is_a?(Hash)
      Hash[record.map {|k, v|
             if v.is_a?(Hash) && v.has_key?('_promise')
               promise = v.fetch('_promise')
               value = @promise_store.fetch_promise(promise['type'], promise['id'])

               if value
                 [k, value]
               else
                 Log.warn("Failed to resolve promise: #{promise.inspect}")
                 ["FAILED_PROMISE::#{promise['type']}", promise['id']]
               end
             else
               resolved = resolve_promises(v, opts)
               [k, resolved]
             end}.compact]

    elsif record.is_a?(Array)
      record.map {|v| resolve_promises(v, opts)}
    else
      record
    end
  end


  def prune_result(record, opts = {})
    if record.is_a?(Hash)
      Hash[record.map {|k, v|
             if k.start_with?("_")
               # Drop our underscored "temporary" properties.
               nil
             else
               [k, prune_result(v, opts)]
             end}.compact]

    elsif record.is_a?(Array)
      record.map {|v| prune_result(v, opts)}.select {|elt|
        if opts[:discard_failed_promises]
          ok = !elt.is_a?(Hash) || !elt.keys.find {|s| s.is_a?(String) && s.start_with?("FAILED_PROMISE")}

          if !ok
            Log.warn("Pruning object: #{elt.inspect}")
          end

          ok
        else
          true
        end
      }
    else
      record
    end
  end


  def put(record_type, record)
    record['external_ids'] ||= []
    record['external_ids'] << {
      'jsonmodel_type' => 'external_id',
      'external_id' => record['id'],
      'source' => 'CIDER DB'
    }

    if @stores.fetch(record_type).has_key?(record.fetch('id'))
      existing_record = @stores.fetch(record_type)[record.fetch('id')]
      Log.warn("ID collision between #{existing_record.inspect} and #{record.inspect}")
      return false
    end

    @stores.fetch(record_type)[record.fetch('id')] = record
    true
  end

  def get(record_type, id)
    @stores.fetch(record_type)[id]
  end

end
