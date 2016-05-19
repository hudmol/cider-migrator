class Dates

  def self.range(start_date, end_date)
    dates = [start_date, end_date].map {|s| Utils.trim(s) }.compact

    return nil if dates.empty?

    {
      'jsonmodel_type' => 'date',
      'date_type' => 'range',
      'begin' => dates[0],
      'end' => dates[1],
      'label' => dates.join(' -- '),
    }
  end

end
