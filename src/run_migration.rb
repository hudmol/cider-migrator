require 'tmpdir'
require 'json'

require_relative "csv_migrator"

basedir = File.expand_path(File.dirname(__FILE__))

Dir.mktmpdir do |dir|
  store = MigrationStore.new(dir)

  tree_store = TreeStore.new(store)

  CSVLoader.new("#{basedir}/samples/csv/Files.csv", FileConverter.new(store, tree_store)).load
  CSVLoader.new("#{basedir}/samples/csv/Boxes.csv", BoxConverter.new(store, tree_store)).load
  CSVLoader.new("#{basedir}/samples/csv/Series.csv", SeriesConverter.new(store, tree_store)).load

  accession_id_unique_check = ColumnUniqueCheck.new("acc_no")
  CSVLoader.new("#{basedir}/samples/csv/Accessions.csv", accession_id_unique_check).load

  accession_converter = AccessionConverter.new(store, accession_id_unique_check.duplicates.map(&:values).flatten)
  CSVLoader.new("#{basedir}/samples/csv/Accessions.csv", accession_converter).load
  CSVLoader.new("#{basedir}/samples/csv/Accessions.csv", AccessionEventConverter.new(store)).load

  creator_person_converter = CreatorPersonConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Creator.csv", creator_person_converter).load

  creator_corporate_entity_converter = CreatorCorporateEntityConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Creator.csv", creator_corporate_entity_converter).load

  creator_family_converter = CreatorFamilyConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Creator.csv", creator_family_converter).load

  contact_person_converter = ContactPersonConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Contacts.csv", contact_person_converter).load

  contact_corporate_entity_converter = ContactCorporateEntityConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Contacts.csv", contact_corporate_entity_converter).load

  location_converter = LocationConverter.new(store)
  CSVLoader.new("#{basedir}/samples/csv/Locations.csv", location_converter).load

  resource_id_unique_check = ColumnUniqueCheck.new("MARC950b")
  CSVLoader.new("#{basedir}/samples/csv/Resources.csv", resource_id_unique_check).load

  resource_converter = ResourceConverter.new(store, resource_id_unique_check.duplicates.map(&:values).flatten)
  CSVLoader.new("#{basedir}/samples/csv/Resources.csv", resource_converter).load

  tree_store.deliver_all_promises!

  store.all_records(:resolve_promises_opts => {:discard_failed_promises => true}) do |record|
    puts record.to_json
  end
end
