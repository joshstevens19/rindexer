# Railway

## One-click Deploy Example

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/Rqrlcf?referralCode=eD4laT)

## Deploy an example project

<https://github.com/joshstevens19/rindexer/tree/master/providers/railway>

1. Clone the relevant directory

  ```bash
  # this will clone the railway directory
  mkdir rindexer-railway && cd rindexer-railway
  git clone \
    --depth=1 \
    --no-checkout \
    --filter=tree:0 \
    https://github.com/joshstevens19/rindexer .
  git sparse-checkout set --no-cone providers/railway .
  git checkout && cp -r providers/railway/* . && rm -rf providers
  ```

2. Initialize a new Railway project

  ```bash
  railway init --name rindexer-example
  ```

3. Create a service and link it to the project
  
  ```bash
  railway up --detach
  railway link --name rindexer-example --enviroment production
  ```

4. Create a Postgres database

  ```bash
  railway add --database postgre-sql
  ```

5. Configure environment variables

  ```bash
  railway open
  ```

- then open the service "Variables" tab and click on "Add Variable Reference" and select `DATABASE_URL`,
- postfix `?sslmode=diable` to the end of the value. It should look like this: `${{Postgres.DATABASE_URL}}?sslmode=disable`,
- hit "Deploy" or Shift+Enter.

6. Create a domain to access GraphQL Playground

  ```bash
  railway domain
  ```

7. Redeploy the service

  ```bash
  railway up
  ```
