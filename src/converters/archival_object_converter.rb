class ArchivalObjectConverter < Converter

  class ArchivalObject
    def from_object(object, db, store, tree_store)
      record = {
        'jsonmodel_type' => 'archival_object',
        'title' => object[:title],
        'id' => object[:number],
        'component_id' => object[:number],
#        'published' => (collection[:processing_status].to_i >= 3).to_s,
#        'restrictions' => (collection[:processing_status].to_i == 3),
        'language' => 'eng',
#        'dates' => build_dates(collection),
        'extents' => build_extents(object, db),
#        'notes' => build_notes(collection),
      }

      parent = db[:object].where(:id => object[:parent]).first
      parent_id = parent[:number]

      tree_store.record_parent(:child => record['id'], :parent => parent_id)

      unless parent[:parent].nil?
        record['parent'] = {'ref' => Migrator.promise('archival_object_uri', parent_id)}
      end

      record['resource'] = {'ref' => Migrator.promise('collection_uri', record['id'])}

      record
    end

    private

    def build_extents(object, db)
      extents = []

      extents << {
         'jsonmodel_type' => 'extent',
         'portion' => 'whole',
         'number' => '1',
         'extent_type' => 'volumes',
      }


      extents
    end

  end


  class Collection < ArchivalObject
    def from_object(object, db, store, tree_store)
      # we don't need this object because the resource mapper is taking care of it
      # just remember the collection_id for the tree store
      tree_store.record_parent(:child => object[:number], :collection => object[:number])
    end
  end


  class Series < ArchivalObject
    def from_object(object, db, store, tree_store)
      store.put_archival_object(super.merge({'level' => 'series'}))
    end
  end


  class Item < ArchivalObject
    def from_object(object, db, store, tree_store)
      store.put_archival_object(super.merge({
        'level' => 'item',
        'subjects' => build_subjects(object, db),
        'instances' => build_instances(object, db)
      }))
    end

    def build_subjects(object, db)
      # only items have these kinds of subjects
      subjects = []

      db[:item_authority_name].where(:item => object[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "authority_name:#{row[:name]}") }
      end

      db[:item_geographic_term].where(:item => object[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "geographic_term:#{row[:term]}") }
      end

      db[:item_topic_term].where(:item => object[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "topic_term:#{row[:term]}") }
      end

      subjects
    end

    def build_instances(object, db)
      if (db[:digital_object].where(:item => object[:id]).count > 0)
        # digital objects can link to a digital object instance
        digital_object = db[:digital_object][:item => object[:id]]
        [{
          'instance_type' => 'digital_object',
          'digital_object' => {
            'ref' => Migrator.promise('digital_object_uri', digital_object[:id].to_s)
          }
        }]
      else
        # TODO link to location instances
        []
      end
    end
  end


  def record_type(object)
    if object[:parent].nil?
      Collection.new
    elsif db[:series].where(:id => object[:id]).count > 0
      Series.new
    else
      Item.new
    end
  end

  def call(store, tree_store)
    Log.info("Going to process #{db[:object].count} archival_object records")

    db[:object].each do |object|
      record_type(object).from_object(object, db, store, tree_store)
    end
  end
end

#
# well, this is good to know:
#
### all objects have a corresponding collection, series or item
#
# select count(*) from object
# where id not in (select id from collection)
#   and id not in (select id from series)
#   and id not in (select id from item);
# +----------+
# | count(*) |
# +----------+
# |        0 |
# +----------+
#
### collections always attach to objects without parents
#
# select count(*) from collection c, object o where c.id = o.id and o.parent is not null;
# +----------+
# | count(*) |
# +----------+
# |        0 |
# +----------+
#
### items can have children
#
# select count(*) from object o, item i, object c where o.id = i.id and c.parent = o.id;
# +----------+
# | count(*) |
# +----------+
# |   183003 |
# +----------+
#
#
# these tables are linkers between items, locations and formats
# they are named in format.class
# there will need to be some sort of mapping to instance_type
# and probably other fields
#
#  count   table
#   2822   container
#  23660   bound_volume
#   1112   three_dimensional_object
#   8227   audio_visual_media
# 113385   document
#  29186   physical_image
#  68220   digital_object
#      0   browsing_object


