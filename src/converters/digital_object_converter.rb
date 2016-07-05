class DigitalObjectConverter < Converter

  def call(store)
    Log.info("Going to process #{db[:digital_object].count} digital object records")

    db[:digital_object].each do |digital_object|
      store.put_digital_object(build_from_digital_object(digital_object, store))
    end
  end

  private

  def build_from_digital_object(digital_object, store)
    item = db[:item][:id => digital_object[:item]]
    object = db[:object][:id => item[:id]]

    record = {
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
    }

    # FIXME Need to generate events:
    # CATALOGUED EVENT 'cataloged_date' => nil, #File Creation Date
    #                  'cataloged_note' => Digital Objects Notes
    # PROSESSED EVENT 'processed_date' => nil, #Stabilization - date
    #                 'processors' => nil, #Stabilization - by
    # RIGHTS_TRANSFERRED EVENT: 'rights_transferred' => nil, #Digital Objects Rights

    record
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
      Log.warn("Digital object doesn't have a pid: #{digital_object[:id]}.  Using object[:number] for digital_object_id instead.")

      if db[:digital_object].where(:item => object[:id]).all.length > 1
        # FIXME these items have multiple digital objects perhaps implying digital object components
        # just fudge some unique digital_object_ids for the moment
        "#{object[:number]} [#{digital_object[:id]}]"
      else
        object[:number]
      end
    end
  end

  def extract_notes(object, item, digital_object, db)
    if digital_object[:notes] && !digital_object[:notes].empty?
      [{
        'jsonmodel_type' => 'note_digital_object',
        'type' => 'note',
        'content' => [digital_object[:notes]]
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

    [{
      'jsonmodel_type '=> 'file_version',
      'file_uri' => url,
      'publish' => extract_file_published(digital_object, db),
      'file_format_name' => extract_file_extension(object, item, digital_object, db),
      'checksum' => digital_object[:checksum],
      'checksum_method' => extract_checksum_method(digital_object, db),
    }]
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
    # FIXME from Creators, implies need to import all old users as well?
    []
  end

  def extract_dates(object, item, digital_object, db)
    # FIXME add created and from audit trail?
    []
  end

  def extract_user_defined(object, item, digital_object, db)
    user_defined = {}

    # string_1 => Digital Objects Location
    location = extract_location(digital_object, db)
    if location[:barcode].length > 255
      Log.warn("Digital object string_1 greater than 255 characters: #{digital_object[:id]}. Truncating.")
    end
    user_defined['string_1'] = location[:barcode][0..254]

    # string_2 => Relationships PID
    relationships = db[:digital_object_relationship].where(:digital_object => digital_object[:id])
    if relationships.count > 0
      pids = relationships.collect{|rel| rel[:pid]}.uniq.sort
      pids = pids.join(", ")

      if pids.length > 255
        Log.warn("Digital object string_2 greater than 255 characters: #{digital_object[:id]}. Truncating.")
      end

      user_defined['string_2'] = pids[0..254]
    end

    # TODO string_3 => Stabilization Applications Other

    # text_1 => Stabilization Notes
    user_defined['text_1'] = digital_object[:stabilization_notes]

    # text_2 => Digital Objects Original Filename
    user_defined['text_2'] = digital_object[:original_filename]

    # TODO enum_1 => Relationships

    # TODO enum_2 => File Extension
    # This is already handled in the file version

    # enum_3 => Stabilization Procedure
    if digital_object[:stabilization_procedure]
      stabilization_procedure = db[:stabilization_procedure][:id => digital_object[:stabilization_procedure]]
      user_defined['enum_3'] = "#{stabilization_procedure[:code]} #{stabilization_procedure[:name]}"
    end

    # enum_4 => Applications Media Image
    if digital_object[:media_app]
      media_app = db[:application][:id => digital_object[:media_app]]

      user_defined['enum_4'] = media_app[:name]
    end

    user_defined
  end

  def extract_location(digital_object, db)
    db[:location][:id => digital_object[:location]]
  end

end