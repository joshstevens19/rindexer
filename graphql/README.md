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

This will replace the resource.zip in the releases folder with this new build.
To deploy you then should follow the instructions about doing a release [here](https://github.com/joshstevens19/rindexer/blob/master/README.md#release).