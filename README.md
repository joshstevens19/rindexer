# rindexer

checklist v1.0:
- finish the documentation
- look into deployments to make it easy to do
- add examples in the repo + callouts in the documentation
- add readmes to the subprojects

bugs:
- Do not create a new postgres client each time on rust projects
- Fix hex string not returning properly on rindexer graphql
- fix last TODOs in the code
- graphql is blocking indexer starting up so making indexing slower

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
