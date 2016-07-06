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

end
