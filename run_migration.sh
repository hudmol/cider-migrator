#!/bin/bash

cd "`dirname "$0"`"

GEM_HOME=lib/gems java -verbose:gc -Dfile.encoding=UTF-8 -Xmx4g -cp 'lib/*' org.jruby.Main --1.9 src/migrator.rb ${1+"$@"}
