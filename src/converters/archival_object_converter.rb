class ArchivalObjectConverter < Converter

  # Tables of interest
  #
  #  object (For title + ID)
  #   collection
  #    series
  #     item
  #
  # Everything is an object
  # Series and items are objects
  # Series are not items (nor vice versa)
  # There are multiple types of items, and they'll (sometimes?) have their own table to provide additional attributes
  # Digital objects aren't direct objects (that is, they don't have an entry in 'object').  They connect via an item.
  #
  # When we're walking these tables we presumably just want to create one
  # archival object for any series or item that isn't a digital object or
  # collection.

  class ArchivalObjectLoader

    def initialize(db, store, tree_store)
      @db = db
      @store = store
      @tree_store = tree_store
    end

    def load(ao_object)
      record_tree_for(ao_object)

      ao_subtype = @db[:series].filter(:id => ao_object[:id]).count > 0 ? :series : :item

      # FIXME: Pretty sparse at this point... mainly focusing on getting the tree stuff right here.
      #
      # Still need to figure out agents, extents, containers, etc.
      ao_json = {
        'id' => ao_object[:number],
        'title' => ao_object[:title],
        'component_id' => extract_component(ao_object),
        'level' => level_for(ao_object),
        'language' => 'eng',
        'dates' => ao_subtype == :series ? build_series_dates(ao_object) : build_item_dates(ao_object),
      }

      ao_json['resource'] = {'ref' => Migrator.promise('collection_uri', ao_object[:number])}
      ao_json['parent'] = {'ref' => Migrator.promise('archival_object_uri', @db[:object].filter(:id => ao_object[:parent]).select(:number).first[:number])}

      @store.put_archival_object(ao_json)
    end

    private

    def build_series_dates(ao_object)
      series = @db[:series].filter(:id => ao_object[:id]).first

      dates = [series[:bulk_date_from], series[:bulk_date_to]].map {|s| Utils.trim(s)}.compact

      case dates.length
      when 0
        # fine for an AO
        []
      when 1
        [Dates.single(dates[0]).merge('label' => 'creation')]
      else
        [Dates.range(dates[0], dates[1]).merge('label' => 'creation')]
      end
    end


    def build_item_dates(ao_object)
      item = @db[:item].filter(:id => ao_object[:id]).first

      dates = [item[:item_date_from], item[:item_date_to]].map {|s| Utils.trim(s)}.compact

      case dates.length
      when 0
        # fine for an AO
        []
      when 1
        [Dates.single(dates[0]).merge('label' => 'creation')]
      else
        [Dates.range(dates[0], dates[1]).merge('label' => 'creation')]
      end
    end

    # There are "Container" types that we don't really understand yet.  For
    # example, see MS009-001.
    def level_for(ao_object)
      if @db[:series].filter(:id => ao_object[:id]).count > 1
        'series'
      else
        # THINKME: Should 'collection: file folder' be a 'collection' or 'file'?
        if @db[:file_folder].filter(:item => ao_object[:id]).count > 1
          'file'
        elsif @db[:container].filter(:item => ao_object[:id]).count > 1
          'container'
        else
          'item'
        end
      end
    end

    def extract_component(ao_object)
      ao_object[:number].split('.')[-1] or raise "No object ID extracted"
    end

    def record_tree_for(ao_object)
      raise "AO found without parent" unless ao_object[:parent]

      # Is this a top-level archival object?
      if @db[:collection].filter(:id => ao_object[:parent]).count > 0
        # top-level archival object
        @tree_store.record_parent(:child => ao_object[:number], :collection => @db[:object].filter(:id => ao_object[:parent]).select(:number).first[:number])
      else
        # item-level
        @tree_store.record_parent(:child => ao_object[:number], :parent => @db[:object].filter(:id => ao_object[:parent]).select(:number).first[:number])
      end
    end

  end


  def call(store, tree_store)
    ao_dataset = build_ao_dataset

    Log.info("Going to process #{ao_dataset.count} archival object records")

    record_count = 0
    ao_dataset.each do |ao_object|
      record_count += 1
      ArchivalObjectLoader.new(db, store, tree_store).load(ao_object)

      $stderr.puts("Processed #{record_count} records") if (record_count % 1000) == 0
    end
  end

  private

  def build_ao_dataset
    dbh = db
    dos = db[:object].filter(:id => db[:digital_object].select(:item))

    # An archival object is...
    #  A series record; or
    #  An item record; and
    #  Not a collection; and
    #  Not an item that is linked to a digital object
    aos = dbh[:object].where {
      Sequel.&(Sequel.|({:id => dbh[:series].select(:id)},
                        {:id => dbh[:item].select(:id)}),
               Sequel.~(:id => dbh[:collection].select(:id)),
               Sequel.~(:id => dos.select(:id)))
    }
  end

end
