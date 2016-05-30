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

    raise "No item for #{digital_object.inspect}" if item.nil?

    object = db[:object][:id => item[:id]]

    raise "No object for #{digital_object.inspect}" if object.nil?

    digital_object_id = extract_digital_object_id(object, item, digital_object, db)

    {
      'id' => digital_object[:id].to_s,
      'digital_object_id' => digital_object_id,
      'title' => object[:title],
      'published' => digital_object_id.match(/[\-\.]/) != nil,
      'digital_object_type' => extract_digital_object_type(object, item, digital_object, db),
      'file_versions' => extract_file_versions(object, item, digital_object, db),
      # CATALOGUED EVENT 'cataloged_date' => nil, #File Creation Date
      # PROSESSED EVENT 'processed_date' => nil, #Stabilization - date
      # ^^ 'processors' => nil, #Stabilization - by
      ## EVENT: 'rights_transferred' => nil, #Rights
    }
  end

  def extract_digital_object_id(object, item, digital_object, db)
     if object.has_key?(:number)
       return object[:number]
     end

     Log.warn("This digital object doesn't have an Item Number (digital_object_id): #{digital_object.inspect}")
     return "FAKE_DIGITAL_OBJECT_ID_#{object[:id]}"
  end

  def extract_digital_object_type(object, item, digital_object, db)
    # I think this is meant to be the dc_type e.g. Text, MovingImage..
    # This would match up with existing enum values
    return nil if item[:dc_type].nil?

    db[:dc_type][:id => item[:dc_type]][:name]
  end

  def extract_file_extension(object, item, digital_object, db)
    db[:file_extension].find(digital_object[:file_extension])[:extension]
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
      'publish' => false, # TODO should this be true?
      'checksum' => digital_object[:checksum],
      'checksum_method' => extract_checksum_method(digital_object, db),
    }]
  end

  def extract_checksum_method(digital_object, db)
    return nil unless digital_object.has_key?(:checksum_app)

    db[:application][:id => digital_object[:checksum_app]][:name]
  end
end