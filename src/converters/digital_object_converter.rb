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

    digital_object_id = extract_digital_object_id(object, item, digital_object, db)

    {
      'id' => digital_object[:id].to_s,
      'digital_object_id' => digital_object_id,
      'title' => object[:title],
      'publish' => extract_published(object, item, digital_object, db),
      'digital_object_type' => extract_digital_object_type(object, item, digital_object, db),
      'file_versions' => extract_file_versions(object, item, digital_object, db),
      'notes' => extract_notes(object, item, digital_object, db),

      # FIXME I was assuming 1.5.0 which would mean the following are events
      # In 1.4.2 these are simple collection_management fields
      # CATALOGUED EVENT 'cataloged_date' => nil, #File Creation Date
      # PROSESSED EVENT 'processed_date' => nil, #Stabilization - date
      # ^^ 'processors' => nil, #Stabilization - by
      ## EVENT: 'rights_transferred' => nil, #Rights
    }
  end

  def extract_published(object, item, digital_object, db)
    # A published digital object contains letters and numbers separated by '.'
    # If unpublished, the components are separated with a '-'
    object[:number].match(/\-/).nil?
  end

  def extract_digital_object_id(object, item, digital_object, db)
    if db[:digital_object].where(:item => object[:id]).all.length > 1
      # FIXME these items have multiple digital objects perhaps implying digital object components
      # just fudge some unique digital_object_ids for the moment
      return "#{object[:number]}-#{digital_object[:pid]}"
    else
      return object[:number]
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

    db[:dc_type][:id => item[:dc_type]][:name]
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
    if (!digital_object.has_key?(:permanent_url) ||
        digital_object[:permanent_url].nil? ||
        digital_object[:permanent_url].empty?)

      return []
    end

    [{
      'jsonmodel_type '=> 'file_version',
      'file_uri' => digital_object[:permanent_url],
      'publish' => false, # FIXME should this be true?
      'file_format_name' => extract_file_extension(object, item, digital_object, db),
      'checksum' => digital_object[:checksum],
      'checksum_method' => extract_checksum_method(digital_object, db),
    }]
  end

  def extract_checksum_method(digital_object, db)
    return nil unless digital_object.has_key?(:checksum_app)

    db[:application][:id => digital_object[:checksum_app]][:name]
  end
end