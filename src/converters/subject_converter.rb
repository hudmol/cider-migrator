class SubjectConverter < Converter

  class Subject
    def from_subjecty_thing(subjecty_thing, type, db, store)
      subject_json = {
        'jsonmodel_type' => 'subject',
        'id' => "#{type.to_s}:#{subjecty_thing[:id]}",
        'source' => 'local',
        'terms' => build_terms(subjecty_thing, type),
        'vocabulary' => Migrator.promise('vocabulary_uri', 'tufts'),
#        'authority_id' => '',
#        'external_documents' => [],
      }

      subject_json['scope_note'] = subjecty_thing[:note] if subjecty_thing[:note]

      store.put_subject(subject_json)
    end


    private

    TERM_TYPE = {
      :collection_subject => 'uniform_title',
      :authority_name => 'uniform_title',
      :geographic_term => 'geographic',
      :topic_term => 'topical'
    }

    TERM_FIELD = {
      :collection_subject => :subject,
      :authority_name => :name,
      :geographic_term => :name,
      :topic_term => :name
    }

    def build_terms(subjecty_thing, type)
      terms = []

      # there is one authority_name whose name is null, hence the UNKNOWN
      terms << {
        'jsonmodel_type' => 'term',
        'term' => subjecty_thing[TERM_FIELD[type]] || "UNKNOWN",
        'term_type' => TERM_TYPE[type],
        'vocabulary' => Migrator.promise('vocabulary_uri', 'tufts'),
      }

      terms
    end

  end

  class Vocabulary
    def from_thin_air(store)
      vocabulary_json = {
        'jsonmodel_type' => 'vocabulary',
        'id' => 'tufts',
        'ref_id' => 'tufts',
        'name' => 'Tufts University'
      }
      store.put_vocabulary(vocabulary_json)
    end
  end

  def call(store)
    Log.info("Creating Vocabulary record out of thin air")
    Vocabulary.new.from_thin_air(store)

    [:collection_subject, :authority_name, :geographic_term, :topic_term].each do |type|
      Log.info("Going to process #{db[type].count} #{type.to_s} records")
      db[type].each do |row|
        Subject.new.from_subjecty_thing(row, type, db, store)
      end
    end

  end
end
