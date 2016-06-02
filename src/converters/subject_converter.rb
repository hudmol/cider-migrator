class SubjectConverter < Converter

  class Subject
    def from_subjecty_thing(subjecty_thing, type, db, store)
      subject_json = {
        'jsonmodel_type' => 'subject',
        'id' => "#{type.to_s}:#{subjecty_thing[:id]}",
        'source' => 'local',
        'scope_note' => subjecty_thing[:note],
        'terms' => build_terms(subjecty_thing, type),
        'vocabulary' => Migrator.promise('vocabulary_uri', 'tufts'),
#        'authority_id' => '',
#        'external_documents' => [],
      }

      store.put_subject(subject_json)
    end


    private

    TERM_TYPE = {
      :authority_name => 'uniform_title',
      :geographic_term => 'geographic',
      :topic_term => 'topical'
    }

    def build_terms(subjecty_thing, type)
      terms = []

      terms << {
        'jsonmodel_type' => 'term',
        'term' => subjecty_thing[:name],
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

    [:authority_name, :geographic_term, :topic_term].each do |type|
      Log.info("Going to process #{db[type].count} #{type.to_s} records")
      db[type].each do |row|
        Subject.new.from_subjecty_thing(row, type, db, store)
      end
    end

  end
end
