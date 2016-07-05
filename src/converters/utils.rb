class Utils

  def self.trim(s)
    return nil if s.nil?

    s = s.strip

    if s.empty?
      nil
    else
      s
    end
  end

  def self.convert_timestamp_for_db(timestamp)
    # timestamp will be a date object which had the local timezone of whoever's
    # running the migration (which is probably Canberra time!) tacked onto it.
    # In reality, it's really a time in US/Eastern, so we'll reparse in that
    # timezone and convert to UTC.
    #
    time_sans_timezone = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    sdf = java.text.SimpleDateFormat.new("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("America/New_York"))

    corrected_java_time = sdf.parse(time_sans_timezone)

    result = Time.at((corrected_java_time.get_time / 1000.0).to_i).getutc.strftime('%Y-%m-%d %H:%M:%S')

    result
  end

end

