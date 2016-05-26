class ResourceConverter < Converter

  # collections don't seem to have ids :(
  # they have permanent urls
  # and in at least one record the processing_notes column contains mentions of what looks like an id 'MS001'
  # but that doesn't seem to be stored anywhere

  # and the AO equiv seems to be 'object', but they don't seem to link up to collections
  # oh wait:

  # collection_relationship.collection > object.id
  # collection_relationship.predicate ...
  # oh, the collection_relationship table is empty :(

  # got it collection and object share ids, so for example:

  # select * from collection where id = 82;
  # select * from object where id = 82;

  # refer to the same thing
  # the object record has number (id_0), and title

  # processing_status
  # +----+------------+---------------------------------------------------------------------------------------------------+
  # | id | name       | description                                                                                       |
  # +----+------------+---------------------------------------------------------------------------------------------------+
  # |  1 | minimal    | This collection is minimally processed and may not be available for research                      |
  # |  2 | partial    | This collection is partially processed                                                            |
  # |  3 | restricted | This collection is processed, but some materials may be restricted and not available for research |
  # |  4 | open       | This collection is processed and open for research                                                |
  # +----+------------+---------------------------------------------------------------------------------------------------+

  # FIXME:
  # collections don't seem to have any extent info - resources require an extent
  # mapping says extents are derived
  # need to look at the cider code for this

  # FIXME:
  # a bunch of collections don't have bulk_date_from or bulk_date_to
  # but resources require at least one date

  class Resource
    def from_collection(collection, db, store)
      obj = db[:object].where(:id => collection[:id]).first
      resource_json = {
        'jsonmodel_type' => 'resource',
        'title' => obj[:title],
        'id' => obj[:number],
        'id_0' => obj[:number],
        'published' => (collection[:processing_status].to_i >= 3).to_s,
        'restrictions' => (collection[:processing_status].to_i == 3),
        'level' => 'collection',
        'resource_type' => 'collection',
        'language' => 'eng',
        'dates' => build_dates(collection),
        'ead_id' => obj[:number],
        'ead_location' => collection[:permanent_url],
        'extents' => build_extents(collection),
        'notes' => build_notes(collection),
      }

      store.put_resource(resource_json)
    end

    def build_dates(collection)
      dates = [collection[:bulk_date_from], collection[:bulk_date_to]].map {|s| Utils.trim(s)}.compact

      case dates.length
      when 0
        # bogus placeholder so we can validate
        [Dates.single('1970').merge('label' => 'creation')]
      when 1
        [Dates.single(dates[0]).merge('label' => 'creation')]
      else
        [Dates.range(dates[0], dates[1]).merge('label' => 'creation')]
      end
    end


    def build_extents(collection)
      # fake extent as a placeholder
      [{
         'jsonmodel_type' => 'extent',
         'portion' => 'whole',
         'number' => '1',
         'extent_type' => 'volumes',
       }]
    end


    def build_notes(collection)
      notes = []

      if collection[:organization]
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'arrangement',
          'content' => [collection[:organization]],
        }
      end

      if collection[:history]
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'custodhist',
          'content' => [collection[:history]],
          'publish' => false,
        }
      end

      if collection[:processing_notes]
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'processinfo',
          'content' => [collection[:processing_notes]],
        }
      end

      if collection[:scope]
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'scopecontent',
          'content' => [collection[:scope]],
        }
      end

      notes
    end

  end


  def call(store)
    Log.info("Going to process #{db[:collection].count} resource records")

    db[:collection].each do |collection|
      Resource.new.from_collection(collection, db, store)
    end
  end
end
