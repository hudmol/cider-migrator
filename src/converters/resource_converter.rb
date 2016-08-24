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

    # Constant from db[:documentation]
    HAS_DOCUMENTATION = 1

    def from_collection(collection, db, store)
      obj = db[:object].where(:id => collection[:id]).first
      resource_json = {
        'jsonmodel_type' => 'resource',
        'title' => obj[:title],
        'id' => obj[:number],
        'id_0' => obj[:number],

        # Force publish to true - https://www.pivotaltracker.com/story/show/123569271
        'publish' => true,
        'user_defined' => {
          'boolean_1' => (collection[:documentation] == HAS_DOCUMENTATION),
        },
        'restrictions' => (db[:item]
                            .filter(:id => db[:enclosure].filter(:ancestor => collection[:id]).select(:descendant))
                            .where {Sequel.~(:restrictions => 1)}.count > 0),
        'level' => 'collection',
        'resource_type' => 'collection',
        # FIXME: there might be more than one language, what to do?
        'language' => db[:collection_language].where(:collection => collection[:id]).first[:language],
        'dates' => build_dates(collection, obj[:number], db),
        'ead_id' => obj[:number],
        'ead_location' => collection[:permanent_url],
        'extents' => Extents.build_extents(obj, db),
        'notes' => build_notes(collection, db),
        'subjects' => build_subjects(collection, db),
        'linked_agents' => build_linked_agents(collection, db)
      }.merge(build_audit_info(obj, db))

      store.put_resource(resource_json)
    end

    private

    def build_dates(collection, number, db)
      dates = []

      # bulk dates > date_type = bulk, date_label = creation
      bulk_date = Dates.range(collection[:bulk_date_from], collection[:bulk_date_to])
      if bulk_date
        dates << bulk_date.merge({'date_type' => 'bulk', 'label' => 'creation'})
      end

      dates << Dates.enclosed_range(db, collection[:id])

      dates.compact!

      if dates.empty?
        dates << Dates.single('1453').merge({'date_type' => 'bulk', 'label' => 'existence'})
      end

      dates
    end


    def build_notes(collection, db)
      notes = []

      if collection[:organization]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'arrangement',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => collection[:organization],
            }
          ]
        }
      end

      if collection[:history]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'custodhist',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => collection[:history],
            }
          ],
        }
      end

      if collection[:processing_notes]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'processinfo',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => collection[:processing_notes],
            }
          ]
        }
      end

      if collection[:notes]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'odd',
          'label' => 'Internal notes',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => collection[:notes],
            }
          ]
        }
      end

      if collection[:scope]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'scopecontent',
          'publish' => false,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => false,
              'content' => collection[:scope],
            }
          ]
        }
      end

      content = []
      db[:collection_material].where(:collection => collection[:id]).each do |row|
        content << row[:material]
      end
      unless content.empty?
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'relatedmaterial',
          'publish' => true,
          'subnotes' => content.map {|note|
              {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => note,
              }
            }
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
          'publish' => true,
          'content' => content,
        }
      end


      # note_bioghist from the text of the biog/hist note in
      # the primary linked agent record.
      primary_agent_hist_note = db[:collection_record_context]
        .join(:record_context, :id => :collection_record_context__record_context)
        .filter(:collection_record_context__collection => collection[:id])
        .filter(:collection_record_context__is_primary => '1')
        .filter(Sequel.~(:record_context__history => nil))
        .select(:record_context__history).first

      if primary_agent_hist_note
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'bioghist',
          'publish' => true,
          'subnotes' => [{
                           'jsonmodel_type' => 'note_text',
                           'publish' => true,
                           'content' => primary_agent_hist_note[:history]
                         }]
        }
      end


      if collection[:processing_status]
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'processinfo',
          'label' => 'Processing status',
          'publish' => true,
          'subnotes' => [
            {
              'jsonmodel_type' => 'note_text',
              'publish' => true,
              'content' => db[:processing_status].filter(:id => collection[:processing_status]).first[:description],
            }
          ]
        }
      end

      accession_numbers = db[:item]
        .join(:enclosure, :enclosure__descendant => :item__id)
        .filter(:enclosure__ancestor => collection[:id])
        .exclude(:item__accession_number => nil)
        .select(:accession_number).distinct(:accession_number)

      if accession_numbers.count > 0
        notes << {
          'jsonmodel_type' => 'note_multipart',
          'type' => 'odd',
          'label' => 'Accessions',
          'publish' => false,
          'subnotes' => [{
                           'jsonmodel_type' => 'note_text',
                           'publish' => false,
                           'content' => accession_numbers.map{|row|
                            row[:accession_number]
                           }.compact.join(", ")
                         }]
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


  def call(store)
    Log.info("Going to process #{db[:collection].count} resource records")

    db[:collection].each do |collection|
      Resource.new.from_collection(collection, db, store)
    end
  end
end
