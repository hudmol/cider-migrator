class AgentSourceParser

  URL_REGEX = /^https?:/i

  def read_url(words)
    return if words.empty?
    url = words.shift
    raise "Invalid URL: #{url} :: #{words}" unless url =~ URL_REGEX
    url.gsub(/\.$/, '')
  end

  def read_text(words)
    result = []

    while (!words.empty? && words[0] !~ URL_REGEX)
      result << words.shift
    end

    result.join(' ')
  end

  def read_accessed_note(words)
    if !words.empty? && (words[0].start_with?('(') || words[0].start_with?('[') || words[0] =~ /accessed|retrieved/i)
      result = []

      while (!words.empty? && words[0] !~ URL_REGEX)
        word = words.shift
        result << word

        break if word.end_with?('.') && word !~ /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/
      end

      result.join(' ')
    else
      nil
    end
  end

  def clean(s)
    # Some URLs run together with the punctuation of the previous sentence.
    s = s.gsub(/([^(])(https?:)/, '\1 \2')

    # A few run ons like '2014).Office...'
    s = s.gsub(/\.Office/, '. Office')

    # Missing space before accessed note
    s = s.gsub(/statement\(accessed/, 'statement (accessed')

    # Spacing problem
    s = s.gsub('cohen/ </p>', 'cohen/</p>')

    s = s.gsub('Accessed at  http://dl.tufts.edu/catalog/tufts:UP049.001.001.00002 on August 7, 2013.',
               'http://dl.tufts.edu/catalog/tufts:UP049.001.001.00002 Accessed on August 7, 2013.')

    s = s.gsub('index and images, FamilySearch (https://familysearch.org/pal:/MM9.1.1/N4DN-JQC : accessed 07 Oct 2014)',
               'index and images, FamilySearch https://familysearch.org/pal:/MM9.1.1/N4DN-JQC accessed 07 Oct 2014)')

    s
  end

  def trim_to_nil(s)
    result = s.strip

    if result.empty?
      nil
    else
      result
    end
  end

  # entry = <url> <text>
  #       | <text> <url> [<accessed note>]
  def parse(s)
    s = clean(s)

    words = s.split(' ')

    result = []

    while !words.empty?
      if words[0] =~ URL_REGEX
        # <url> <text>
        result << {url: read_url(words), text: trim_to_nil(read_text(words))}
      else
        # <text> <url> [<accessed note>]
        entry = read_text(words)
        url = read_url(words)
        accessed_note = read_accessed_note(words)
        if accessed_note
          entry += ' ' + accessed_note
        end

        result << {url: url, text: trim_to_nil(entry)}
      end
    end

    result
  end
end
