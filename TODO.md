# rindexer

checklist v1.0:
- do benchmarks with a few different projects
- add a favicon to the docs

bugs:
- Do not create a new postgres client each time on rust projects
- fix the environment variables for RPC urls
- csv needs to have last seen block in a .rindexer folder file somewhere so it can be picked up again
- graphql default first 1000 when not supplied

nice to have:
- add examples in the repo + callouts in the documentation
- look at the final unwraps
- look at the final clones
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
