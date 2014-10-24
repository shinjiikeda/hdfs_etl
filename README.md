# HdfsEtl

     producers => kafka => hdfs_etl => hdfs

## Installation

Add this line to your application's Gemfile:

    gem 'hdfs_etl'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install hdfs_etl

## Usage

    ## daemon start
    $ bundle exec jruby -J-Xmx2048m -J-server -J-XX:+UseConcMarkSweepGC hdfs_etl.rb --logfile /tmp/hdfs_etl.log --daemon >/tmp/hdfs_etl.out 2>&1 &

## Contributing

1. Fork it ( http://github.com/shinjiikeda/hdfs_etl/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
