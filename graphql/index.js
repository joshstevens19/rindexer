const express = require('express')
const { postgraphile } = require("postgraphile");
const {makeWrapResolversPlugin} = require("graphile-utils");

const args = process.argv.slice(2);

if (args.length < 5) {
    console.error("Usage: postgraphile <connectionString> <schemas> <port> <page_limit> <timeout>>");
    process.exit(1);
}

const connectionString = args[0];
const schemas = args[1].split(",");
const port = parseInt(args[2]);
let graphqlPageLimit = parseInt(args[3]);
let graphqlTimeout = parseInt(args[4]);

const byteaToHex =  makeWrapResolversPlugin(
    (context) => {
        return context;
    },
    ({ scope }) =>
        async (resolver, user, args, context, _resolveInfo) => {
            if (typeof args === "object") {
                const first = args['first'];
                const last = args['last'];

                const firstValue = first ? parseInt(first) : 0;
                const lastValue = last ? parseInt(last) : 0;

                if (firstValue > graphqlPageLimit || lastValue > graphqlPageLimit) {
                    throw new Error(`Pagination limit exceeded. Maximum allowed is ${graphqlPageLimit}.`)
                }
            }

            // always add a limit on the amount you can bring back if last is defined
            // then let the resolver handle it as limits has been handled above already
            if (args['last'] === undefined) {
                args['first'] = args['first'] ? args['first'] : graphqlPageLimit;
            }

            let result = await resolver();
            if (result && typeof result === "string") {
                // it is a bytea need to turn back to a hex
                result = result.startsWith('\\x') ? result.replace('\\x', '0x') : result;
            }
            return result;
        },
);

const options = {
    watchPg: true,
    host: "localhost",
    disableDefaultMutations: true,
    dynamicJson: true,
    cors: true,
    retryOnInitFail: true,
    enableQueryBatching: true,
    noIgnoreIndexes: true,
    ignoreIndexes: true,
    enhanceGraphiql: false,
    graphiql: false,
    disableQueryLog: true,
    pgSettings: {
        statement_timeout: graphqlTimeout,
    },
    appendPlugins: [byteaToHex],
};



const htmlContent = (endpoint) => `
    <div style="width: 100%; height: 100%;" id='embedded-sandbox'></div>
    <script src="https://embeddable-sandbox.cdn.apollographql.com/_latest/embeddable-sandbox.umd.production.min.js"></script> 
    <script>
      new window.EmbeddedSandbox({
        target: '#embedded-sandbox',
        initialEndpoint: '${endpoint}',
      });
    </script>
`;

const app = express()
app.use(express.json())
app.use(postgraphile(connectionString, schemas, options))

app.get('/playground', (req, res) => {
    res.send(htmlContent(`http://localhost:${port}/graphql`));
});

app.listen(port, "0.0.0.0", () => {
    console.log(`GraphQL endpoint: http://localhost:${port}/graphql`);
    console.log(
        `GraphiQL Playground endpoint: http://localhost:${port}/playground`
    );
});