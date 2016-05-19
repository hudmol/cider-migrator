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

end

