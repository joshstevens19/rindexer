# rindexer

checklist v1.0:
- look at doing the bulk insert + copy route
- look into setting your own schema for the database using diesel
- finish the documentation (note about block timestamp)
- add a getting started guide for rust / no-code
- look into deployments to make it easy to do

bugs:
- name cant have `-` in it

nice to have:
- look into PK with tx hash and tx index and log index to make it unique and not have to worry about duplicates
- investigate indexing contracts that are deployed within an event onchain
  - register a manifest defining factory including address, event, parameter name, and ABI
  - when it emits the event of the factory start a new log polling for the new contract
  - emit the log through the same event register as the event defined in the manifest

future features:
- block timestamp indexing
- investigate handle advanced top of head reorgs process
- cron registering for networks to fire
- multiple ABIs merged into one
- look into load balancing of RPCs
- look into internal caching to make things faster
- POC with shadow events using foundry as you index
