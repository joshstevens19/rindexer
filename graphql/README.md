# graphql

This is the graphql package which uses postgraphile plugin for support rindexer GraphQL, we package it into a binary and run it within the rindexer.

## Package up and release

To package up the graphql package you can run the following command:

```bash
npm run build
```

once done then run

```bash
chmod +x ./bundle-resource.sh # only need this the first time
./bundle-resource.sh
```