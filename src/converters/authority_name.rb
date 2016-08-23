class AuthorityName

  def self.person?(name, note)
    if note =~ /man|woman|male|female/i
      # Anything with "man" "woman" "male" or "female" in the notes field is
      # likely a person, not a corporate entity
      return true
    end

    if note =~ /last name only|first name unknown/i
      # "Last name only" and "first name unknown" are people
      return true
    end

    if name =~ /\A(Department|Dept)/i
      # Anything starting with Department or Dept
      # is a corporate entity
      return false
    end

    if name =~ /congress/i
      # Anything with the word "Congress" in the name (NOT the notes)
      # is a corporate entity
      return false
    end

    # Anything with (Organization) in it is a corporate entity
    if name =~ /\(Organization\)/
      return false
    end

    # Anything with "author" in the name or notes is likely a person
    # (exceptions noted; make sure to limit to "author" or you'll accidentally
    # get "authority" as well, and those should stay corporate)
    if name =~ /authority/i || note =~ /authority/i
      return false
    end
    if name =~ /author/i || note =~ /author/i
      return true
    end

    # Anything with an ampersand standing alone ot the html encoding for an
    # ampersand (&amp; ) is almost certainly a corporate entity
    # (ex: Skidmore, Owings & Merrill). (Careful to limit to
    # space-ampersand-space or &amp; or you'll also get the html encoding for
    # special characters, like &#00E4;)
    if name =~ /\s&\s|&amp;/
      return false
    end

    if name =~ /[0-9]{4}/
      # life dates
      return true
    end

    bits = name.split(',')

    if bits.length > 2
      false
    elsif bits.length <= 1
      false
    elsif bits[0].include?(" ")
      false
    elsif bits[1].split(" ").length > 3
      false
    else
      true
    end
  end

  def self.subject?(name)
    if name =~ /--/
      return true
    end

    false
  end

end