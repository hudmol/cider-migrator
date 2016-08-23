require_relative 'authority_name'

class ArchivalObjectConverter < Converter

  CPUS_TO_MELT = 8

  RESTRICTION_NOTES = {
    1 => "Open for research.",
    2 => "20 years from date of creation.",
    3 => "75 years from the date of creation.",
    4 => "Physical condition.",
  }

  class ArchivalObject

    def from_object(object, db)
      record = {
        'jsonmodel_type' => 'archival_object',
        'title' => object[:title],
        'id' => object[:number],
        'component_id' => object[:number],
        'publish' => true,
        'language' => build_language(object, db),
        'dates' => build_dates(object, db)
      }

      parent = db[:object].where(:id => object[:parent]).first
      parent_id = parent[:number]

      # The position of the current AO is the number of siblings with smaller
      # numbers than it (positions are zero-indexed).
      record['position'] = db[:object].filter(:parent => object[:parent]).where { number < object[:number] }.count

      record_parent(:child => record['id'], :parent => parent_id)

      unless parent[:parent].nil?
        record['parent'] = {'ref' => Migrator.promise('archival_object_uri', parent_id)}
      end

      record['resource'] = {'ref' => Migrator.promise('collection_uri', record['id'])}

      record.merge!(build_audit_info(object, db))

      record
    end

    attr_reader :parent_info

    private

    def record_parent(args)
      @parent_info ||= []
      parent_info << args
    end

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


    def build_dates(object, db)
      dates = []

      dates << Dates.enclosed_range(db, object[:id])

      dates.compact!

      dates
    end


    def build_language(object, db)
      db[:collection_language]
        .where(:collection => db[:collection]
                                .join(:enclosure, :enclosure__ancestor => :collection__id)
                                .filter(:enclosure__descendant => object[:id])
                                .select(:enclosure__ancestor))
        .first[:language]
    end
  end


  class Collection < ArchivalObject
    def from_object(object, db)
      # we don't need this object because the resource mapper is taking care of it
      # just remember the collection_id.
      record_parent(:child => object[:number], :collection => object[:number])
      nil
    end
  end


  class Series < ArchivalObject
    def from_object(object, db)
      @series = db[:series].where(:id => object[:id]).first

      record = super.merge({
        'level' => 'series',
        'notes' => build_notes(object, db),
      })

      apply_restriction_fields(record, db)

      record
    end

    private

    def build_dates(object, db)
      dates = super

      # bulk dates > date_type = bulk, date_label = creation
      dates << Dates.range(@series[:bulk_date_from], @series[:bulk_date_to], 'creation', 'bulk')

      dates.compact!

      dates
    end


    def apply_restriction_fields(record, db)
      # derive the restrictions for the series

      restrictions = db[:item]
        .join(:item_restrictions, :item_restrictions__id => :item__restrictions)
        .filter(:item__id => db[:enclosure].filter(:ancestor => @series[:id]).select(:descendant))
        .distinct(:item_restrictions__id)
        .select(:item_restrictions__id)
        .order(Sequel.desc(:item_restrictions__id))
        .map {|row| row[:id]}

      if restrictions.length > 0
        record['notes'] ||= []
        record['notes'] << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'accessrestrict',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => restrictions.collect {|r|
                RESTRICTION_NOTES[r]
              }.join(" "),
            }
          ]
        }

        if restrictions[0] > 1
          record['restrictions_apply'] = true
        end
      end
    end

    def build_notes(object, db)
      notes = []

      if @series[:description]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'scopecontent',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => @series[:description],
            }
          ]
        }
      end

      if @series[:arrangement]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'arrangement',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => @series[:arrangement],
            }
          ]
        }
      end

      if @series[:notes]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'odd',
          'label' => 'Internal notes',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => @series[:notes],
            }
          ]
        }
      end

      notes
    end

  end

  class Subseries < Series
    def from_object(object, db)
      super.merge({'level' => 'subseries'})
    end
  end


  class Item < ArchivalObject
    def from_object(object, db)
      @item = db[:item].where(:id => object[:id]).first

      record = super.merge({
                    'level' => find_level(object, item, db),
                    'subjects' => build_subjects(object, db),
                    'instances' => build_instances(object, db),
                    'restrictions_apply' => item[:restrictions] > 1,
                    'notes' => build_notes(object, item, db),
                  })

      merge_authority_name_links(record, object, item, db)

      record
    end

    def item
      @item
    end

    def find_level(object, item, db)
      if item[:is_group] == '1' # is_group is an enum and comes through as a string
        # Groups always migrate as files
        # See: https://www.pivotaltracker.com/n/projects/1592339
        'file'
      elsif item[:dc_type] == 1 || db[:container].filter(:item => object[:id]).count > 0
        # If the DC type is 'collection' (or the type of the item is), we'll emit a file
        'file'
      else
        'item'
      end
    end

    def build_subjects(object, db)
      # only items have these kinds of subjects
      subjects = []

      db[:item_geographic_term].where(:item => object[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "geographic_term:#{row[:term]}") }
      end

      db[:item_topic_term].where(:item => object[:id]).each do |row|
        subjects << { 'ref' => Migrator.promise('subject_uri', "topic_term:#{row[:term]}") }
      end

      subjects
    end

    def merge_authority_name_links(record, object, item, db)
      agent_links = []
      subjects = []

      db[:object]
        .join(:item_authority_name, :item_authority_name__item => :object__id)
        .join(:authority_name, :authority_name__id => :item_authority_name__name)
        .filter(:item => object[:id])
        .select(:authority_name__id, :authority_name__name, :item_authority_name__role)
        .each do |link|

        if AuthorityName.subject?(link[:name])
          subjects << {
            'ref' => Migrator.promise('authority_name_subject_uri', "authority_name:#{link[:id].to_s}")
          }
        else
          role = (link[:role] == 'creator') ? 'creator' : 'subject'
          agent_links << {
            'role' => role,
            'ref' => Migrator.promise('authority_name_agent_uri', link[:id].to_s)
          }
        end
      end

      record['subjects'] ||= []
      record['subjects'].concat(subjects)

      record['linked_agents'] ||= []
      record['linked_agents'].concat(agent_links)
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

    def build_notes(object, item, db)
      notes = []

      # restrictions == 1 is no restrictions
      if item[:restrictions]

        content = RESTRICTION_NOTES[item[:restrictions]]

        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'accessrestrict',
          'publish' => false,
          'subnotes' => [
                         {
                           'jsonmodel_type' => 'note_text',
                           'publish' => false,
                           'content' => content,
                         }
                        ]
        }

      end

      if item[:abstract]
        notes << {
          'jsonmodel_type' => 'note_singlepart',
          'type' => 'abstract',
          'publish' => false,
          'content' => [item[:abstract]]
        }
      end

      if item[:accession_number]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'odd',
          'label' => 'accessions, numbers separated by commas',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => item[:accession_number],
            }
          ]
        }
      end

      if item[:citation]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'prefercite',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => item[:citation],
            }
          ]
        }
      end

      if item[:description]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'scopecontent',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => item[:description],
            }
          ]
        }
      end

      if item[:volume] || item[:issue]
        label_bits = []
        label_bits << "volume" if item[:volume]
        label_bits << "issue" if item[:issue]

        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'odd',
          'label' => label_bits.join(", ").capitalize,
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => [item[:volume], item[:issue]].compact.join(", "),
            }
          ]
        }
      end

      notes
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
      :file_folder => 'mixed_materials',         # ???
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

    def build_dates(object, db)
      dates = super

      # add the item's dates as a creation single/inclusive date
      # only from date so show as single
      item_date_from = Utils.trim(item[:item_date_from])
      item_date_to = Utils.trim(item[:item_date_to])
      if item_date_from && item_date_to.nil?
        add_date_if_unique(dates, Dates.single(item[:item_date_from].strip).merge({
                                                                                    'label' => 'creation',
                                                                                    'date_type' => 'single',
                                                                                    'certainty' => item[:circa] == '1' ? 'approximate' : nil,
                                                                                  }))

      # both dates so show as inclusive
      elsif item_date_from && item_date_to
        date_arr = [item_date_from, item_date_to].sort

        if date_arr[0] != item_date_from
          Log.warn("Item 'from' date is after 'to' date item #{item[:id]} (#{item_date_from} > #{item_date_to})")
        end

        add_date_if_unique(dates, Dates.range(date_arr[0], date_arr[1], 'creation', 'inclusive').merge({
                                                                                                         'certainty' => item[:circa] == '1' ? 'approximate' : nil,
                                                                                                       }))

      # only to date so show as inclusive
      elsif item_date_to
        add_date_if_unique(dates, Dates.range(nil, item_date_to, 'creation', 'inclusive').merge({
                                                                                                  'certainty' => item[:circa] == '1' ? 'approximate' : nil,
                                                                                                }))
      end

      dates
    end


    # we want to avoid duplicate item dates
    def add_date_if_unique(dates, date_to_add)
      dates << date_to_add if dates.none? {|date|  date['expression'] == date_to_add['expression'] }
    end

  end


  def record_type(object)
    if object[:parent].nil?
      Collection.new
    elsif db[:series].where(:id => object[:id]).count > 0
      # If we're nested within another Series, this is actually a Subseries
      if db[:object].filter(:object__id => object[:id]).join(:series, :series__id => :object__parent).count > 0
        Subseries.new
      else
        Series.new
      end
    else
      Item.new
    end
  end


  def call(store, tree_store)
    Log.info("Going to process #{db[:object].count} archival_object records using #{CPUS_TO_MELT} threads")

    # Build up a big list of the IDs we'll process
    id_list = db[:object].select(:id).map {|row| row[:id]}

    # Group our work into chunks of records, with one chunk allocated to each thread
    id_list.each_slice(10).each_slice(CPUS_TO_MELT) do |workset|
      threads = []
      workset.each do |ids|
        threads << Thread.new do
          # Each thread grabs its list of objects, transforms them and returns
          # the resulting record and the AO object it used (which carries the
          # parent/child info)
          objects = db[:object].filter(:id => ids)

          objects.map do |object|
            ao = record_type(object)
            record = ao.from_object(object, db)

            [record, ao]
          end
        end
      end

      # Back in the main thread, gather up the results from our threads and
      # write them out to our storage.
      threads.each do |thread|
        thread.value.each do |record, ao|
          Array(ao.parent_info).each do |info|
            tree_store.record_parent(info)
          end

          store.put_archival_object(record) if record
        end
      end
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


