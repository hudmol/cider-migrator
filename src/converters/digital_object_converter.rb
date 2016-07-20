require 'time'

class DigitalObjectConverter < Converter

  def call(store)
    Log.info("Going to process #{db[:digital_object].count} digital object records")

    db[:digital_object].each do |digital_object|
      item = db[:item][:id => digital_object[:item]]
      object = db[:object][:id => item[:id]]

      store.put_digital_object(build_from_digital_object(object, item, digital_object, store))

      build_events(object, item, digital_object).each do |event|
        store.put_event(event)
      end
    end
  end

  private

  def build_from_digital_object(object, item, digital_object, store)
    {
      'id' => digital_object[:id].to_s,
      'digital_object_id' => extract_digital_object_id(object, item, digital_object, db),
      'level' => extract_level(object, item, digital_object, db),
      'publish' => extract_published(object, item, digital_object, db),
      'digital_object_type' => extract_digital_object_type(object, item, digital_object, db),
      'restrictions' => extract_restrictions(object, item, digital_object, db),
      'title' => object[:title],
      'linked_agents' => extract_linked_agents(object, item, digital_object, db),
      'dates' => extract_dates(object, item, digital_object, db),
      'file_versions' => extract_file_versions(object, item, digital_object, db),
      'notes' => extract_notes(object, item, digital_object, db),
      'user_defined' => extract_user_defined(object, item, digital_object, db),
    }.merge(extract_audit_info(object, db))
  end


  def extract_restrictions(object, item, digital_object, db)
    if item[:restrictions]
      # From db[:item_restrictions]
      # 1 none
      # 2-4 restricted
      item[:restrictions] > 1
    else
      false
    end
  end

  def extract_level(object, item, digital_object, db)
    # Nothing direct, but can likely come up with a rule based on format,
    # recognizing all items in data05 are item-level (image/work)
    # and most in darkarchive are tarballs (collection)

    if digital_object[:format]
      format = db[:format][:id => digital_object[:format]]

      if format[:name].match(/[image|jpg|photo]/)
        'image'
      elsif format[:name].match(/[iso|tar|zip]/)
        'collection'
      else
        'work'
      end
    else
      location = extract_location(digital_object, db)

      if location[:barcode] == 'data05'
        'work'
      elsif location[:barcode] == 'darkarchive'
        'collection'
      else
        nil
      end
    end
  end

  def extract_published(object, item, digital_object, db)
    # A published digital object contains letters and numbers separated by '.'
    # If unpublished, the components are separated with a '-'
    object[:number].match(/\-/).nil?
  end

  def extract_digital_object_id(object, item, digital_object, db)
    if digital_object[:pid] && !digital_object[:pid].empty?
      if db[:digital_object].where(:pid => digital_object[:pid]).count > 1
        Log.warn("Digital object pid is not unique: #{digital_object[:id]}. Appending id to the digital_object_id.")

        "#{digital_object[:pid]} [#{digital_object[:id]}]"
      else
        digital_object[:pid]
      end
    else
      Log.warn("Digital object doesn't have a pid: #{digital_object[:id]}. Generating a random value for digital_object_id instead.")

      SecureRandom.hex
    end
  end

  def extract_notes(object, item, digital_object, db)
    notes = []

    # The checksum might have been too large to fit in the ArchivesSpace
    # checksum field (for example, if it's actually a list of checksums of
    # multiple files).  Store it in a note instead.
    if digital_object[:checksum] && digital_object[:checksum].length >= 255
      notes << digital_object[:checksum]
    end

    if digital_object[:notes] && !digital_object[:notes].empty?
      notes << digital_object[:notes]
    end

    if !notes.empty?
      [{
        'jsonmodel_type' => 'note_digital_object',
        'type' => 'note',
        'content' => notes
      }]
    else
      []
    end
  end

  def extract_digital_object_type(object, item, digital_object, db)
    # I think this is meant to be the dc_type e.g. Text, MovingImage..
    # This would match up with existing enum values
    return nil if item[:dc_type].nil?

    dc_type_name = db[:dc_type][:id => item[:dc_type]][:name]

    # convert camel case to snake case to better match with existing enums
    dc_type_name.gsub(/(.)([A-Z])/,'\1_\2').downcase
  end

  def extract_file_extension(object, item, digital_object, db)
    if digital_object[:file_extension]
      ext = db[:file_extension][:id => digital_object[:file_extension]][:extension]
      # drop the leading '.'
      ext.sub(/^\./, "")
    else
      nil
    end
  end

  def extract_file_versions(object, item, digital_object, db)
    url = digital_object[:permanent_url]

    if !url || url.empty?
      url = 'example://no-url-available'
    end

    result = {
      'jsonmodel_type '=> 'file_version',
      'file_uri' => url,
      'publish' => extract_file_published(digital_object, db),
      'file_format_name' => extract_file_extension(object, item, digital_object, db),
      'checksum_method' => extract_checksum_method(digital_object, db),
    }

    if digital_object[:checksum]
      if digital_object[:checksum].length < 255
        result['checksum'] = digital_object[:checksum]
      else
        result['checksum'] = 'See attached note for full checksum information'
      end
    end

    [result]
  end

  def extract_checksum_method(digital_object, db)
    return nil unless digital_object.has_key?(:checksum_app)

    db[:application][:id => digital_object[:checksum_app]][:name]
  end

  def extract_file_published(digital_object, db)
    # if location = data05: TRUE;
    # if location = darkarchive:FALSE
    location = extract_location(digital_object, db)

    location[:barcode] == 'data05'
  end

  def extract_linked_agents(object, item, digital_object, db)
    begin
      creator = db[:log].
        filter(:audit_trail => object[:audit_trail], :action => 'create').
        order(:id).select(:staff).first.
        fetch(:staff)

      [{
         'role' => 'creator',
         'ref' => Migrator.promise('staff_uri', creator.to_s),
       }]
    rescue
      Log.warn("Digital object doesn't have a creator: #{digital_object[:id]}. Skipping linked agent for creator role.")
      []
    end
  end

  def extract_dates(object, item, digital_object, db)
    # FIXME add created and from audit trail?
    []
  end

  def extract_user_defined(object, item, digital_object, db)
    user_defined = {}

    # string_1 => Digital Objects Original Filename
    user_defined['string_1'] = digital_object[:original_filename]

    # text_1 => Digital Objects Location
    location = extract_location(digital_object, db)
    user_defined['text_1'] = location[:barcode]

    # DISABLED AS WE DON'T NEED TO MIGRATE THESE ANYMORE
    # https://www.pivotaltracker.com/story/show/121256939
    #
    # text_2 => Relationships PID
    # relationships = db[:digital_object_relationship].
    #                   join(:relationship_predicate, :relationship_predicate__id => :digital_object_relationship__predicate).
    #                   where(:digital_object_relationship__digital_object => digital_object[:id]).
    #                   select(:digital_object_relationship__pid, :relationship_predicate__predicate)
    # if relationships.count > 0
    #   pids = relationships.collect{|rel| "#{rel[:predicate]} #{rel[:pid]}" }.uniq.sort
    #   user_defined['text_2'] = pids.join("; ")
    # end

    # text_3 => Other Applications
    applications = db[:digital_object_application].where(:digital_object => digital_object[:id])
    if applications.count > 0
      application_names = applications.collect{|app| app[:application] }
      user_defined['text_3'] = application_names.join("; ")
    end

    user_defined
  end

  def extract_location(digital_object, db)
    db[:location][:id => digital_object[:location]]
  end


  def extract_audit_info(object, db)
    audit_fields = {}

    created = db[:log].join(:staff, :staff__id => :log__staff)
                .where(:log__audit_trail => object[:audit_trail])
                .and(:log__action => 'create').first

    if created
      audit_fields['created_by'] = "#{created[:first_name]} #{created[:last_name]}"
      audit_fields['create_time'] = Utils.convert_timestamp_for_db(created[:timestamp])
    end

    updated = db[:log].join(:staff, :staff__id => :log__staff)
                .where(:log__audit_trail => object[:audit_trail])
                .and(:log__action => 'update')
                .order(:log__id).last

    if updated
      audit_fields['last_modified_by'] = "#{updated[:first_name]} #{updated[:last_name]}"
      audit_fields['user_mtime'] =Utils.convert_timestamp_for_db(updated[:timestamp])
    end

    audit_fields
  end


  def build_events(object, item, digital_object)
    events = []

    # not all records have a 'created' log item,
    # so just grab their first one...
    create_timestamp = db[:log].
      filter(:audit_trail => object[:audit_trail]).
      order(:id).select(:timestamp).first.
      fetch(:timestamp)

    # processed event
    processed_event = build_processed_event(digital_object, create_timestamp)
    events << processed_event if processed_event

    # capture event
    # Event type = capture; event date/time specifier = UTC;
    # agent link role = executing program;
    # agent = all software options currently in CIDER need to migrate as agents
    if digital_object[:media_app]
      events << {
        'event_type' => 'capture',
        'timestamp' => Utils.convert_timestamp_for_db(create_timestamp),
        'linked_agents' => [{
                              'ref' => Migrator.promise('application_uri', digital_object[:media_app].to_s),
                              'role' => 'executing_program',
                            }],
        'linked_records' => [{
                               'ref' => Migrator.promise('digital_object_uri', digital_object[:id].to_s),
                               'role' => 'outcome',
                             }]

      }
    end

    # Event type = virus check;
    # event date/time specifier = UTC;
    # agent link role = executing program;
    # agent = all software options currently in CIDER need to migrate as agents
    if digital_object[:virus_app]
      events << {
        'event_type' => 'virus_check',
        'timestamp' => Utils.convert_timestamp_for_db(create_timestamp),
        'linked_agents' => [{
                              'ref' => Migrator.promise('application_uri', digital_object[:virus_app].to_s),
                              'role' => 'executing_program',
                            }],
        'linked_records' => [{
                              'ref' => Migrator.promise('digital_object_uri', digital_object[:id].to_s),
                              'role' => 'outcome',
                             }]
      }
    end

    events
  end


  def build_processed_event(digital_object, create_timestamp)
    # only build an event if digital object as a value for the following fields
    required_fields = [:stabilized_by, :stabilization_notes, :stabilization_date, :stabilization_procedure]
    if required_fields.all? {|attr| digital_object[attr].nil? || digital_object[attr] == ''}
      return nil
    end

    processed_event = {
      'event_type' => 'processed',
      'linked_agents' => [],
      'linked_records' => [{
                             'ref' => Migrator.promise('digital_object_uri', digital_object[:id].to_s),
                             'role' => 'outcome',
                           }]
    }

    if digital_object[:stabilization_notes] && digital_object[:stabilization_notes] != ""
      processed_event['outcome_note'] = digital_object[:stabilization_notes]
    else
      processed_event['outcome'] = 'pass'
    end

    if digital_object[:stabilization_date] && digital_object[:stabilization_date] != ""
      processed_event['date'] = {
        'date_type' => 'single',
        'label' => 'event',
        'begin' => digital_object[:stabilization_date],
      }
    else
      processed_event['timestamp'] = Utils.convert_timestamp_for_db(create_timestamp)
    end

    if digital_object[:stabilized_by]
      processed_event['linked_agents'] << {
        'ref' => Migrator.promise('staff_uri', digital_object[:stabilized_by].to_s),
        'role' => 'implementer',
      }
    end

    if digital_object[:stabilization_procedure]
      processed_event['linked_agents'] << {
        'ref' => Migrator.promise('stabilization_procedure_uri', digital_object[:stabilization_procedure].to_s),
        'role' => 'executing_program',
      }
    end

    if processed_event['linked_agents'].empty?
      # link to the DCA Staff agent so we don't lose any stabilization information
      processed_event['linked_agents'] << {
        'ref' => Migrator.promise('dca_staff_agent_uri', 'dca_staff'),
        'role' => 'implementer',
      }
    end

    processed_event
  end
end
