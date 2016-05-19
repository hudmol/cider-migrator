class Log

  def self.warn(msg)
    $stderr.puts("\n\n*** WARNING: #{msg}\n")
  end

  def self.info(msg)
    $stderr.puts("\n\n*** INFO: #{msg}\n")
  end

end

