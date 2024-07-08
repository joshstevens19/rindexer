# rindexer

checklist v1.0:
- do benchmarks with a few different projects
- review postgraphile config again
  - cluster-workers
  - simple-collections <omit|both|only>
  - --sort-export
  - enable https://github.com/graphile/pg-simplify-inflector
- add ability to add indexes to the database (this in turn is defining what you want to query on so will fix ordering speed)
- add validation method into the yaml file to check certain things are mapped correctly

bugs:
- details::address: 0xBa5e05cb26b78eDa3A2f8e3b3814726305dcAc83 should be able to pass in an array
- on normal ContractDetails::address you can still filter on indexed topics so allow it
- RUST PROJECT ISSUE - // TODO - this is not correct (this will be fixed if we remove some repeated info from the code)

lastly:
- update the docs to include the new features + changes
  - write relationships docs + write code to add to the dependency events
- cleanup the code into smaller files and functions
- add unit tests as much as possible (time gap 3 hours)

nice to have:
- look at the final unwraps
- look into PK with tx hash and tx index and log index to make it unique and not have to worry about duplicates
