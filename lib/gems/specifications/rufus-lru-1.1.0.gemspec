# -*- encoding: utf-8 -*-
# stub: rufus-lru 1.1.0 ruby lib

Gem::Specification.new do |s|
  s.name = "rufus-lru"
  s.version = "1.1.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["John Mettraux"]
  s.date = "2016-05-09"
  s.description = "LruHash class, a Hash with a max size, controlled by a LRU mechanism"
  s.email = ["jmettraux@gmail.com"]
  s.homepage = "http://github.com/jmettraux/rufus-lru"
  s.licenses = ["MIT"]
  s.require_paths = ["lib"]
  s.rubyforge_project = "rufus"
  s.rubygems_version = "2.1.9"
  s.summary = "A Hash with a max size, controlled by a LRU mechanism"

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rspec>, [">= 3.4.0"])
    else
      s.add_dependency(%q<rspec>, [">= 3.4.0"])
    end
  else
    s.add_dependency(%q<rspec>, [">= 3.4.0"])
  end
end
