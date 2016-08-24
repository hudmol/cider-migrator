class Extents

  # sadly, only volumes exists in the default enum, the others will be added
  EXTENT_TYPES = {
    'Bound volume' => 'volumes',
    'Artifact' => 'artifact',
    'Audio-visual media' => 'audiovisual_media',
    'Digital objects' => 'digital_objects',
  }


  def self.build_extents(obj, db)
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
      group_by = unit_type[:name] == 'Digital objects' ? 'referent_object' : 'location'

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

end
