# rindexer

checklist v1.0:
- do benchmarks with a few different projects

bugs:
- fix the environment variables for RPC urls
- csv needs to have last seen block in a .rindexer folder file somewhere so it can be picked up again

nice to have:
- look at the final unwraps
- look into PK with tx hash and tx index and log index to make it unique and not have to worry about duplicates
- add ability to add indexes to the database
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
