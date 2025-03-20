# TODO (Native Balance Indexing)

1. Retry blocks that fail to fetch
2. Integrate to existing stream processor 
    - Csv
    - Postgres
    - Streams
3. Add progress monitor for historical indexing progress tracking
4. Find a way to respect rate-limits?
5. Decide if batching the call at request level (probably should -> more boilerplate though)