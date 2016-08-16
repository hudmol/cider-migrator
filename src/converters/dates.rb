class Dates

  def self.single(date)
    {
      'jsonmodel_type' => 'date',
      'date_type' => 'single',
      'begin' => Utils.trim(date),
      'expression' => date,
    }
  end


  def self.range(start_date, end_date, label = nil)
    dates = [start_date, end_date].map {|s| Utils.trim(s) }.compact

    return nil if dates.empty?

    {
      'jsonmodel_type' => 'date',
      'date_type' => 'range',
      'begin' => dates[0],
      'end' => dates[1],
      'expression' => dates.join(' -- '),
      'label' => label,
    }
  end


  def self.enclosed_range(db, id)
    # derived dates > date_type = inclusive, date_label = creation
    # creation dates are derived. see:
    # lib/CIDER/Schema/Result/ObjectWithDerivedFields
    dates_query = "select min(i.item_date_from) as date_from, max(i.item_date_from) as date_from_to, " +
      "max(i.item_date_to) as date_to, i.circa " +
      "from item i, enclosure e where i.id = e.descendant and e.ancestor = #{id}"
    result = db.fetch(dates_query).first
    if result[:date_from]
      from = [result[:date_from], result[:date_from_to], result[:date_to]].compact.min[0,4]
      to = [result[:date_from], result[:date_from_to], result[:date_to]].compact.max[0,4]
      is_circa = result[:circa] == '1'
      range(from, to).merge({
        'date_type' => 'inclusive',
        'label' => 'creation',
        'certainty' => is_circa ? 'approximate' : nil,
      })
    end
  end

end
