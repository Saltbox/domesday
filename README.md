# domesday

Experience API (xAPI) Tabulation Reports Script

[![Build Status](https://travis-ci.org/saebyn/domesday.svg)](https://travis-ci.org/saebyn/domesday)


## Installation

You will need https://github.com/technomancy/leiningen to build domesday.

    $ git clone https://github.com/Saltbox/domesday.git
    $ cd domesday/
    $ lein uberjar


## Usage

    $ cd target/
    $ java -jar domesday-0.1.0-SNAPSHOT-standalone.jar -A [2014-04-30T11:38:16-0700] -Z [2014-04-30T12:38:16-0700] -e [https://url.to.my.lrs.example.com/TCAPI/statements] -u [basicusername] -p [basicpassword] first_group_filename.txt second_group_filename.txt


Each filename passed on the command line will be treated as a new group to
tabulate on. Domesday will use the first line of each file as the name of the
group in the output. Domesday will use each subsequent line in each file as
an agent definition.


## Options

    -A [ISO8601 formatted start time]

        Date and time to provide to your LRS as the `since`
        parameter.

    -Z [ISO8601 formatted end time]

        Date and time to provide to your LRS as the `until`
        parameter.

    -u [HTTP Basic username for your LRS]

    -p [HTTP Basic password for your LRS]

    -e [LRS statements URL]

    -q [URL query string]

        Additional GET parameters to pass to your LRS's
        statements endpoint.

## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

## License

Copyright © 2014 Saltbox Services

Distributed under the Apache License. See LICENSE file for the full license text.
