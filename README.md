## elephant twin [![Build Status](https://secure.travis-ci.org/twitter/elephant-twin.png)](http://travis-ci.org/twitter/elephant-twin)
Elephant Twin is a framework for creating indexes in Hadoop. 

It provides a generic abstraction for how indexes get created and represented; specific implementations can create sparse or dense indexes, use a number of different on-disk formats (we provide examples of simple Lucene-based indexes, as well as Hadoop MapFiles; but anything else is possible -- using HBase for indexing comes to mind, for example).

At Twitter, this code is responsible for generating a number of indexes.

An example of a dense index would be the Lucene indexes of user bios, interests, screennames, and other attributes that are used to power the Twitter user search. Code for creating Lucene indexes is available under the elephant-twin-lucene subproject. 

An example of a sparse index would be the MapFile-based indexes of event types per LZO block in log files, which we use to significantly reduce the number of bytes needed for Hadoop and Pig jobs that only need subsets of all possible Twitter events to be analyzed. Code for creating LZO indexes and using them in InputFormats and Pig Loaders is available in the separate elephant-twin-lzo project.

## Building

Standard mvn build -- try "mvn compile", "mvn test", "mvn install".

Note that you will need Thrift version 0.5.0 installed (we use thrift to encode metadata about generated indexes).

## Issues

Have a bug? Please create an issue here on GitHub!

https://github.com/twitter/elephant-twin/issues

## Versioning

For transparency and insight into our release cycle, releases will be numbered with the follow format:

`<major>.<minor>.<patch>`

And constructed with the following guidelines:

* Breaking backwards compatibility bumps the major
* New additions without breaking backwards compatibility bumps the minor
* Bug fixes and misc changes bump the patch

For more information on semantic versioning, please visit http://semver.org/.

## Authors

* Yu Xu
* Jimmy Lin
* Dmitriy Ryaboy

We are thankful to our other contributors in the git-log.

## License

Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
