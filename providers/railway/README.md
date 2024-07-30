# Railway

## One-click Deploy Example

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/Rqrlcf?referralCode=eD4laT)

## Deploy an example project

<https://github.com/joshstevens19/rindexer/tree/master/providers/railway>

1. Clone the relevant directory

  ```shell-session
  # this will clone the railway directory
  git clone --no-checkout --depth=1 --filter=tree:0 https://github.com/joshstevens19/rindexer .
  git sparse-checkout set --no-cone providers/railway
  git checkout
  ```

2. Change into the directory

  ```shell-session
  cd providers/railway
  ```

3. Initialize a new Railway project

  ```shell-session
  railway init --name rindexer-example
  ```

4. Create a service and link it to the project
  
  ```shell-session
  railway up --detach
  railway link --name rindexer-example --enviroment production
  ```

5. Create a Postgres database

  ```shell-session
  railway add --database postgre-sql
  ```

6. Configure environment variables

  ```shell-session
  railway open
  ```

- then open the service "Variables" tab and click on "Add Variable Reference" and select `DATABASE_URL`,
- postfix `?sslmode=diable` to the end of the value. It should look like this: `${{Postgres.DATABASE_URL}}?sslmode=disable`,
- hit "Deploy" or Shift+Enter.

8. Create a domain to access GraphQL Playground

  ```shell-session
  railway domain
  ```

9. Redeploy the service

  ```shell-session
  railway up
  ```
