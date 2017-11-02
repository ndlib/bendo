#! /bin/bash -xe

# ruby 2.4.1
version=2.4.1
cd /usr/local/src
curl -sO https://cache.ruby-lang.org/pub/ruby/2.4/ruby-$version.tar.gz
tar zxvf ruby-$version.tar.gz
cd ruby-$version
./configure
make
make install

# ruby-gems
version=2.6.12
cd ..
curl -sO https://rubygems.org/rubygems/rubygems-$version.tgz
tar zxvf rubygems-$version.tgz
cd rubygems-$version
/usr/local/bin/ruby setup.rb

# chef-solo
gem install bundler chef ruby-shadow --no-ri --no-rdoc
