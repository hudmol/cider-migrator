require 'digest/sha1'

require_relative 'agent_source_parser'

class AgentConverter < Converter
  # Build up our agent record pulling in specific behaviour from our
  # subclasses as needed.
  class BaseAgent
    def from_record_context(record_context, db, store, agent_registry)
      primary_name = build_name(record_context[:name_entry]).merge('authorized' => true)

      alternate_names = db[:record_context_alternate_name].filter(:record_context => record_context[:id]).map {|alternate_name|
        build_name(alternate_name[:name])
      }

      {
        'id' => record_context[:record_id],
        'names' => [primary_name, *alternate_names],
        'dates_of_existence' => build_dates(record_context),
        'notes' => build_notes(record_context),
        'related_agents' => build_related_agents(record_context, db),
        'external_documents' => build_external_documents(record_context, db),
      }
    end

    def from_authority_name(authority_name, db, store, agent_registry)
      name = authority_name[:name]
      note = authority_name[:note]

      # split anything in a name after a backwards or forwards slash
      # e.g., /printer\author - into the notes field and out of the name,
      # for both people and corporate entities
      if name =~ /\\/
        parts = name.split(/\\/)
        name = parts[0]
        note = [parts[1..-1], note].flatten.compact.join("; ")
      end

      primary_name = build_name(name).merge('authorized' => true)

      agent = {
        'id' => "authority_name:#{authority_name[:id]}",
        'names' => [primary_name],
      }

      if note && note != ""
        agent['notes'] = [{
          'jsonmodel_type' => 'note_bioghist',
          'subnotes' => [{
            'jsonmodel_type' => 'note_text',
            'content' => note
          }]
        }]
      end

      agent
    end

    private

    def build_dates(record_context)
      dates = [record_context[:date_from], record_context[:date_to]].map {|s| Utils.trim(s)}.compact

      # Probably always makes sense to think of an agent's life as a range,
      # not a single date.
      case dates.length
      when 0
        []
      else
        [Dates.range(dates[0], dates[1]).merge('label' => 'existence',
                                               'certainty' => (record_context[:circa] == '1') ? 'approximate' : nil)]
      end
    end

    def build_notes(record_context)
      notes = []

      bioghist = {
        'jsonmodel_type' => 'note_bioghist',
        'publish' => true,
        'subnotes' => []
      }

      if record_context[:abstract]
        bioghist['subnotes'] << {
          'jsonmodel_type' => 'note_abstract',
          'content' => [record_context[:abstract]],
        }
      end

      if record_context[:history]
        bioghist['subnotes'] << {
          'jsonmodel_type' => 'note_text',
          'content' => record_context[:history],
        }
      end

      if record_context[:structure_notes]
        bioghist['subnotes'] << {
          'jsonmodel_type' => 'note_text',
          'content' => record_context[:structure_notes],
        }
      end

      if record_context[:context]
        bioghist['subnotes'] << {
          'jsonmodel_type' => 'note_text',
          'content' => record_context[:context],
        }
      end

      unless bioghist['subnotes'].empty?
        notes << bioghist
      end

      notes
    end

    def build_related_agents(record_context, db)
      db[:record_context_relationship]
        .join(:record_context_relationship_type, :id => :record_context_relationship__type)
        .join(Sequel.as(:record_context, :target), :record_context_relationship__related_entity => :target__id)
        .filter(:record_context_relationship__record_context => record_context[:id])
        .select(Sequel.as(:record_context_relationship_type__name, :relationship_type),
                :record_context_relationship__related_entity,
                :record_context_relationship__date_from,
                :record_context_relationship__date_to,
                Sequel.as(:target__record_id, :related_identifier),
               ).map {|related_record_context|
        relationship = related_agent_relationship(related_record_context)
        if relationship
          relationship.merge('ref' => Migrator.promise('record_context_uri',
                                                       related_record_context.fetch(:related_identifier)))
        end
      }.compact
    rescue
      $stderr.puts("FIXME: blow up on #{record_context.inspect}")
      raise $!
    end

    def related_agent_relationship(related_record_context)
      raise "Unsure: #{related_record_context.inspect}"
    end


    def build_external_documents(record_context, db)
      external_documents = []

      rows = db[:record_context_source].filter(:record_context => record_context[:id])

      source_parser = AgentSourceParser.new

      url_count = 1
      rows.each do |row|
        docs = source_parser.parse(row[:source])
        docs.each do |doc|
          external_documents << {
            'title' => doc[:text] || "Untitled",
            'location' => doc[:url] || suffix_string("example://no-url-available", url_count)
          }

          url_count += 1
        end
      end

      external_documents
    end

    # It turns out <titles, locations> must be unique within the external
    # document list.  Suffix our URLs to ensure this.
    def suffix_string(base, count)
      if count == 1
        # no suffix for the first one
        base
      else
        base + "-#{count}"
      end
    end

  end

  class AgentCorporateEntity < BaseAgent
    def from_record_context(record_context, db, store, agent_registry)
      agent = super.merge({
                            'jsonmodel_type' => 'agent_corporate_entity',
                          })
      store.put_agent_corporate_entity(agent)
      agent_registry.record_agent(agent)
    end

    def from_authority_name(authority_name, db, store, agent_registry)
      agent = super.merge({
                            'jsonmodel_type' => 'agent_corporate_entity',
                          })

      if (uri = agent_registry.find_existing(agent))
        # We'll reuse it
      else
        uri = store.put_agent_corporate_entity(agent)
        agent_registry.record_agent(agent)
      end

      store.deliver_promise('authority_name_agent_uri', authority_name[:id].to_s, uri)
    end

    def build_name(name_string)
      {
        'sort_name_auto_generate' => true,
        'jsonmodel_type' => 'name_corporate_entity',
        'primary_name' => name_string,
        'source' => 'local',
      }
    end

    def related_agent_relationship(related_record_context)
      case related_record_context[:relationship_type]
      when 'isPrecededBy'
        {
          'jsonmodel_type' => 'agent_relationship_earlierlater',
          # FIXME have I got this around the right way?
          'relator' => 'is_later_form_of',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'isFollowedBy'
        {
          'jsonmodel_type' => 'agent_relationship_earlierlater',
          # FIXME: have I got this around the right way?
          'relator' => 'is_earlier_form_of',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'isPartOf', 'isChildOf'
        {
          'jsonmodel_type' => 'agent_relationship_subordinatesuperior',
          'relator' => 'is_subordinate_to',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'hasPart', 'isParentOf'
        {
          'jsonmodel_type' => 'agent_relationship_subordinatesuperior',
          'relator' => 'is_superior_to',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'hasMember', 'isMemberOf', 'isAssociatedWith', 'reportsTo', 'hasReport'
        {
          'jsonmodel_type' => 'agent_relationship_associative',
          'relator' => 'is_associative_with',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      else
        super
      end
    end
  end

  class AgentFamily < BaseAgent
    def from_record_context(record_context, db, store, agent_registry)
      agent = super.merge({
                            'jsonmodel_type' => 'agent_family',
                          })
      store.put_agent_family(agent)
      agent_registry.record_agent(agent)
    end

    def build_name(name_string)
      {
        'sort_name_auto_generate' => true,
        'jsonmodel_type' => 'name_family',
        'family_name' => name_string,
        'source' => 'local',
      }
    end

    def related_agent_relationship(related_record_context)
      case related_record_context[:relationship_type]
      when 'isAssociatedWith'
        {
          'jsonmodel_type' => 'agent_relationship_associative',
          'relator' => 'is_associative_with',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'isParentOf', 'isSpouseOf'
        Log.warn("This relationship seems weird: #{related_record_context.inspect}")
        return nil
      else
        super
      end
    end

  end

  class AgentPerson < BaseAgent
    def from_record_context(record_context, db, store, agent_registry)
      agent = super.merge({
                            'jsonmodel_type' => 'agent_person',
                          })
      store.put_agent_person(agent)
      agent_registry.record_agent(agent)
    end

    def from_authority_name(authority_name, db, store, agent_registry)
      agent = super.merge({
                            'jsonmodel_type' => 'agent_person',
                          })

      if (uri = agent_registry.find_existing(agent))
        # We'll reuse it
      else
        uri = store.put_agent_person(agent)
        agent_registry.record_agent(agent)
      end

      store.deliver_promise('authority_name_agent_uri', authority_name[:id].to_s, uri)
    end

    def build_name(name_string)
      (last_name, rest_name) = name_string.split(', ', 2)

      {
        'sort_name_auto_generate' => true,
        'jsonmodel_type' => 'name_person',
        'primary_name' => last_name,
        'rest_of_name' => rest_name,
        'name_order' => 'inverted',
        'source' => 'local',
      }
    end

    def related_agent_relationship(related_record_context)
      case related_record_context[:relationship_type]
      when 'isParentOf'
        {
          'jsonmodel_type' => 'agent_relationship_parentchild',
          'relator' => 'is_parent_of',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'isChildOf'
        {
          'jsonmodel_type' => 'agent_relationship_parentchild',
          'relator' => 'is_child_of',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      when 'isMemberOf', 'isAssociatedWith', 'isPartOf', 'isSpouseOf', 'isFollowedBy', 'isGrandparentOf', 'isGrandchildOf'
        {
          'jsonmodel_type' => 'agent_relationship_associative',
          'relator' => 'is_associative_with',
          'dates' => Dates.range(related_record_context[:date_from], related_record_context[:date_to], 'agent_relation'),
        }
      else
        super
      end
    end

  end

  # Some agents overlap between the RCR tables and the authority_name tables.  Link those agents together

  class AgentRegistry

    def initialize
      @seen_agents = {}
    end

    def record_agent(agent)
      @seen_agents[hash_for(agent)] = agent.fetch('uri')
    end

    def find_existing(agent)
      result = @seen_agents.fetch(hash_for(agent), nil)

      if result
        Log.info("Found existing RCR agent for #{agent.inspect} - #{result}")
      end

      result
    end

    private

    def hash_for(agent)
      key = {
        :jsonmodel_type => agent['jsonmodel_type'],
        :authorized_name => agent['names'].find {|name| name['authorized']}.sort
      }

      raise if key.values.include?(nil)

      Digest::SHA1.hexdigest(key.to_json)
    end

  end

  RC_TYPE_TO_AGENT_TYPE = {
    1 => AgentCorporateEntity.new,
    2 => AgentFamily.new,
    3 => AgentPerson.new,
  }

  def person?(name, note)
    if note =~ /man|woman|male|female/i
      # Anything with "man" "woman" "male" or "female" in the notes field is
      # likely a person, not a corporate entity
      return true
    end

    if note =~ /last name only|first name unknown/i
      # "Last name only" and "first name unknown" are people
      return true
    end

    if name =~ /congress/i
      # Anything with the word "Congress" in the name (NOT the notes)
      # is a corporate entity
      return false
    end

    # Anything with "author" in the name or notes is likely a person
    # (exceptions noted; make sure to limit to "author" or you'll accidentally
    # get "authority" as well, and those should stay corporate)
    if name =~ /authority/i || note =~ /authority/i
      return false
    end
    if name =~ /author/i || note =~ /author/i
      return true
    end

    # Anything with an ampersand standing alone ot the html encoding for an
    # ampersand (&amp; ) is almost certainly a corporate entity
    # (ex: Skidmore, Owings & Merrill). (Careful to limit to
    # space-ampersand-space or &amp; or you'll also get the html encoding for
    # special characters, like &#00E4;)
    if name =~ /\s&\s|&amp;/
      return false
    end

    if name =~ /[0-9]{4}/
      # life dates
      return true
    end

    bits = name.split(',')

    if bits.length > 2
      false
    elsif bits.length <= 1
      false
    elsif bits[0].include?(" ")
      false
    elsif bits[1].split(" ").length > 3
      false
    else
      true
    end
  end

  def call(store)
    Log.info("Going to process #{db[:record_context].count} agent records")

    agent_registry = AgentRegistry.new

    db[:record_context].each do |record_context|
      agent_type = RC_TYPE_TO_AGENT_TYPE.fetch(record_context[:rc_type])

      agent_type.from_record_context(record_context, db, store, agent_registry)
    end

    db[:authority_name].each do |authority|
      if person?(authority[:name], authority[:note])
        AgentPerson.new.from_authority_name(authority, db, store, agent_registry)
      else
        AgentCorporateEntity.new.from_authority_name(authority, db, store, agent_registry)
      end
    end
  end
end
