require 'yaml'

LANGUAGES = YAML.load_file('language_iso639_2.yml')

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
        # FIXME: there might be more than one language, what to do?
        'language' => db[:collection_language].where(:collection => collection[:id]).first[:language],
        'dates' => build_dates(collection, obj[:number], db),
        'ead_id' => obj[:number],
        'ead_location' => collection[:permanent_url],
        'extents' => build_extents(obj, db),
        'notes' => build_notes(collection, db),
        'subjects' => build_subjects(collection, db),
        'linked_agents' => build_linked_agents(collection, db)
      }

      store.put_resource(resource_json)
    end

    private

    def build_dates(collection, number, db)
      dates = [collection[:bulk_date_from], collection[:bulk_date_to]].map {|s| Utils.trim(s)}.compact

      case dates.length
      when 0
        # dates are sometimes derived. see:
        # lib/CIDER/Schema/Result/ObjectWithDerivedFields
        dates_query = "select min(i.item_date_from) as date_from, max(i.item_date_from) as date_from_to, " +
          "max(i.item_date_to) as date_to " +
          "from object o, item i where o.id = i.id and o.number like '#{number}%'"
        result = db.fetch(dates_query).first
        if result[:date_from]
          [Dates.range(result[:date_from], (result[:date_to] || result[:date_from_to])).merge('label' => 'creation')]
        else
          # this will fail validation - resources must have a date
          [Dates.single('1970').merge('label' => 'creation')]
        end
      when 1
        [Dates.single(dates[0]).merge('label' => 'creation')]
      else
        [Dates.range(dates[0], dates[1]).merge('label' => 'creation')]
      end
    end


    # sadly, only volumes exists in the default enum, the others will be added
    EXTENT_TYPES = {
      'Bound volume' => 'volumes',
      'Artifact' => 'artifact',
      'Audio-visual media' => 'audiovisual_media',
      'Digital objects' => 'digital_objects',
    }


    def build_extents(obj, db)
      extents = []

      # extent is a derived field in cider. see:
      # lib/DBIx/Class/DerivedElements.pm#27

      # object > object_location > location > unit_type

      # mysql> select * from unit_type;
      # +----+--------------------+--------+
      # | id | name               | volume |
      # +----+--------------------+--------+
      # |  1 | 1.20 cubic ft.     |   1.20 |
      # |  2 | 0.55 cubic ft.     |   0.55 |
      # |  3 | 0.50 cubic ft.     |   0.50 |
      # |  4 | 0.40 cubic ft.     |   0.40 |
      # |  5 | 0.25 cubic ft.     |   0.25 |
      # |  6 | 0.20 cubic ft.     |   0.20 |
      # |  7 | Bound volume       |   NULL |
      # |  8 | Artifact           |   NULL |
      # |  9 | Audio-visual media |   NULL |
      # | 10 | Digital objects    |   NULL |
      # | 11 | zero               |   0.00 |
      # | 13 | 2.40 cubic ft.     |   2.40 |
      # | 14 | 3.60 cubic ft.     |   3.60 |
      # | 15 | 4.8 cubic ft.      |   4.80 |
      # | 16 | 7.2 cubic ft.      |   7.20 |
      # +----+--------------------+--------+
      # kooky huh?

      # it works out the total volume in cubic feet like this:
      # select volume, location
      # from object_location, unit_type, location
      # where unit_type.id = location.unit_type
      #   and object_location.location = location.id
      #   and object_location.object = $id
      #   and volume is not null
      #   and volume > 0
      # group by location, volume

      # then it adds up the values in the volume column
      # the group by is curious - i think it means an object can refer to a location many times
      # but you only count it once

      volume_query = "select volume, location " +
        "from object_location, unit_type, location " +
        "where unit_type.id = location.unit_type " +
        "and object_location.location = location.id " +
        "and object_location.object = #{obj[:id]} " +
        "and volume is not null " +
#        "and volume > 0 " +
        "group by location, volume"

      volume = 0.0;
      at_least_one = false
      db.fetch(volume_query) do |row|
        at_least_one = true
        volume += row[:volume]
      end

      # there are a few collections that have one zero volume location
      if at_least_one
        extents << {
         'jsonmodel_type' => 'extent',
         'portion' => 'part',
         'number' => volume.round(2).to_s,
         'extent_type' => 'cubic_feet',
        }
      end

      # ok good. now for non-volume types
      db[:unit_type].where(:volume => nil).each do |unit_type|
        group_by = unit_type[:name] == 'Digital object' ? 'referent_object' : 'location'

        count_query = "select count(distinct #{group_by}) as cnt " +
          "from object_location, unit_type, location " +
          "where unit_type.id = location.unit_type " +
          "and object_location.location = location.id " +
          "and object_location.object = #{obj[:id]} " +
          "and unit_type = #{unit_type[:id]}"

        count = db.fetch(count_query).first

        if count[:cnt] > 0
          extents << {
            'jsonmodel_type' => 'extent',
            'portion' => 'part',
            'number' => count[:cnt].to_s,
            'extent_type' => EXTENT_TYPES[unit_type[:name]],
          }
        end
      end

      # if there is only 1 extent then it must be for the whole, no?
      if extents.length == 1
        extents[0]['portion'] = 'whole'
      end

      # temporary fix for the 7 that don't have an extent
      if extents.length == 0
        extents << {
          'jsonmodel_type' => 'extent',
          'portion' => 'whole',
          'number' => '1',
          'extent_type' => 'volumes',
        }
      end

      extents
    end


    def build_notes(collection, db)
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

      content = []
      db[:collection_material].where(:collection => collection[:id]).each do |row|
        content << row[:material]
      end
      unless content.empty?
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'relatedmaterial',
          'content' => content,
        }
      end

      content = []
      db[:collection_language].where(:collection => collection[:id]).each do |row|
        content << LANGUAGES.fetch(fix_language(row[:language]))
      end
      unless content.empty?
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'langmaterial',
          'content' => content,
        }
      end

      notes
    end

    def fix_language(code)
      case code
          when 'grk'
            # Assume we want Modern Greek
            'gre'
          else
            code
      end
    end

    def build_subjects(collection, db)
      subjects = []

      db[:collection_subject].where(:collection => collection[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "collection_subject:#{row[:id]}") }
      end

      subjects
    end

    def build_linked_agents(collection, db)
      linked_agents = []

      db[:collection_record_context]
        .join(:record_context, :id => :collection_record_context__record_context)
        .filter(:collection_record_context__collection => collection[:id])
        .select(Sequel.as(:record_context__record_id, :record_id)).each do |row|

        linked_agents << {
          'role' => 'creator',
          'ref' => Migrator.promise('record_context_uri', row[:record_id])
        }
      end

      linked_agents
    end

  end


  def call(store)
    Log.info("Going to process #{db[:collection].count} resource records")

    db[:collection].each do |collection|
      Resource.new.from_collection(collection, db, store)
    end
  end
end
