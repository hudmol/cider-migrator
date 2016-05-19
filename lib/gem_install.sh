#!/bin/bash

cd "`dirname "$0"`"

GEM_HOME=$PWD/gems java -cp jruby-complete-1.7.18.jar org.jruby.Main --1.9 -S gem install ${1+"$@"}
