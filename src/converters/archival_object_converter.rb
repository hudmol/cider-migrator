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
#        'notes' => build_notes(collection),
      }

      parent = db[:object].where(:id => object[:parent]).first
      parent_id = parent[:number]

      # The position of the current AO is the number of siblings with smaller
      # numbers than it (positions are zero-indexed).
      record['position'] = db[:object].filter(:parent => object[:parent]).where { number < object[:number] }.count

      tree_store.record_parent(:child => record['id'], :parent => parent_id)

      unless parent[:parent].nil?
        record['parent'] = {'ref' => Migrator.promise('archival_object_uri', parent_id)}
      end

      record['resource'] = {'ref' => Migrator.promise('collection_uri', record['id'])}

      record.merge!(build_audit_info(object, db))

      record
    end

    private

    def build_audit_info(object, db)
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
        audit_fields['user_mtime'] = Utils.convert_timestamp_for_db(updated[:timestamp])
      end

      audit_fields
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
        'level' => find_level(object, db),
        'subjects' => build_subjects(object, db),
        'instances' => build_instances(object, db)
      }))
    end

    def find_level(object, db)
      level = 'item'

      item = db[:item].where(:id => object[:id]).first
      if item[:dc_type] == 1 # Collection
        level = 'file'
      end

      level
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

    def build_container(class_object, db)
      format = db[:format].where(:id => class_object[:format]).first

      # FIXME: this mapping is just lazy guesswork, will need review
      #        and we may be going to v1.5 so this will all be wrong anway
      type = if format
               case format[:class]
               when 'bound_volume'
                 'volume'
               else
                 case format[:name]
                 when 'book'
                   'volume'
                 when 'photo frames'
                   'frame'
                 when 'garment'
                   'object'
                 when '16mm', '3/4 -inch', '3/4-inch', 'audio tape', 'sound tape reel', 'tape reels'
                   'reel'
                 when 'CD-Rom', 'audiocassette', 'audiocassettes', 'audiograph', 'Betacam (TM)', 'CD', 'compact disc', 'compact disk', 'DVD'
                   'case'
                 else
                   'box'
                 end
               end
             else
               # some class_objects don't have a format record
               'box'
             end

      location = db[:location].where(:id => class_object[:location]).first
      unit_type = db[:unit_type].where(:id => location[:unit_type]).first

      {
        'jsonmodel_type' => 'container',
        'type_1' => type,
        'barcode_1' => location[:barcode]
      }
    end

    CLASS_INSTANCE = {
      :container => 'mixed_materials', # ???
      :bound_volume => 'books',
      :three_dimensional_object => 'realia',
      :audio_visual_media => 'audio', # or moving_images
      :document => 'text',
      :physical_image => 'graphic_materials',
      :digital_object => 'digital_object',
      :browsing_object => 'digital_object_link', # made up - there aren't any of these anyway
    }

    def build_instances(object, db)
      instances = []

      CLASS_INSTANCE.each_pair do |cider_class, instance_type|
        db[cider_class].where(:item => object[:id]).each do |class_object|
          instance = {
            'jsonmodel_type' => 'instance',
            'instance_type' => instance_type,
          }
          if cider_class == :digital_object
            instance['digital_object'] = {
              'ref' => Migrator.promise('digital_object_uri', class_object[:id].to_s)
            }
          else
            instance['container'] = build_container(class_object, db)
          end

          instances << instance
        end
      end

      instances
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


