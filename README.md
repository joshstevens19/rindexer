# rindexer

checklist v1.0:
- finish the documentation
- add a getting started guide for rust / no-code
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

future features:
- look into setting your own schema for the database using diesel
- investigate indexing contracts that are deployed within an event onchain
  - register a manifest defining factory including address, event, parameter name, and ABI
  - when it emits the event of the factory start a new log polling for the new contract
  - emit the log through the same event register as the event defined in the manifest
- block timestamp indexing - https://ethereum-magicians.org/t/proposal-for-adding-blocktimestamp-to-logs-object-returned-by-eth-getlogs-and-related-requests/11183
- investigate handle advanced top of head reorgs process
- cron registering for networks to fire
- multiple ABIs merged into one
- look into load balancing of RPCs
- look into internal caching of the log results to make things faster if you make changes to your schema to make resyncing faster
- POC with shadow events using foundry as you index
