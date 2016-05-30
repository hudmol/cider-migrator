class TreeStore

  def initialize(store)
    @child_to_parent = {}
    @collections = Set.new
    @store = store
  end


  def record_parent(opts)
    raise "Already have a value!" if @child_to_parent[opts.fetch(:child)]

    parent = opts.fetch(:collection) { opts.fetch(:parent) }
    @child_to_parent[opts.fetch(:child)] = parent

    if opts.has_key?(:collection)
      @collections << opts.fetch(:collection)
    end
  end


  def byte_size
    total = 0

    @collections.each do |collection|
      total += collection.length
    end

    @child_to_parent.each do |child, parent|
      total += child.length
      total += parent.length
    end

    total
  end


  def deliver_all_promises!
    # Each Archival Object needs to know the Resource it belongs to.  Walk our
    # tree of parent/child relationships to work those out.

    # A leaf is a node that isn't a parent of any other record.  I.e. for a
    # mapping like:
    #
    #  {
    #    a => b
    #    b => c
    #    c => d
    # }
    #
    # 'a' is a leaf because it appears on the left but not on the right.

    leaf_nodes = @child_to_parent.keys - @child_to_parent.values

    # As we find which collection each node belongs to, record it to speed up
    # future searches within the same tree.
    collection_cache = {}

    nodes_to_deliver = leaf_nodes.uniq

    while !nodes_to_deliver.empty?
      # Let's not flood the log...
      if rand < 0.001
        Log.info("Nodes left to deliver: #{nodes_to_deliver.length}")
      end
      node = nodes_to_deliver.shift

      children = [node]

      while true
        if (parent_node = @child_to_parent[node])
          if collection_cache[parent_node]
            record_collection_for_children(children, collection_cache[parent_node], collection_cache)
            break
          else
            # Continue up the tree.
            children << parent_node
            node = parent_node
          end
        else
          # Node is a top-level record.  Deliver promises for all children
          # discovered that haven't had their promises delivered yet.

          if @collections.include?(node)
            record_collection_for_children(children, node, collection_cache)
          else
            raise "Found an tree of Archival Object records with no top-level resource: #{children.inspect}"
          end

          break
        end
      end

    end

  end


  def record_collection_for_children(children, collection_node, collection_cache)
    collection_uri = @store.uri_for(:resource, collection_node)

    children.each do |child_node|
      @store.deliver_promise('collection_uri', child_node, collection_uri)
      collection_cache[child_node] = collection_node
    end
  end

end
