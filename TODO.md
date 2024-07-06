# rindexer

checklist v1.0:
- do benchmarks with a few different projects
- review postgraphile config again
  - cluster-workers
  - simple-collections <omit|both|only>
  - --sort-export
  - enable https://github.com/graphile/pg-simplify-inflector

bugs:

nice to have:
- look at the final unwraps
- look into PK with tx hash and tx index and log index to make it unique and not have to worry about duplicates
- add ability to add indexes to the database (this in turn is defining what you want to query on so will fix ordering speed)
- ability for one-to-many relationships
  -     relationships:
        - contract_name: BasePaint
          event: Transfer
          event_input_name: from
          linked_to:
            - contract_name: BasePaint
              event: Approval
              event_input_name: owner
            - contract_name: BasePaint
              event: Approval
              event_input_name: owner
