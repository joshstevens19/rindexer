const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port = 5003;

app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: true }));

app.post('/webhook', (req, res) => {
    const shared_secret = req.headers['x-rindexer-shared-secret'];
    if (shared_secret !== "123") {
        console.log('Shared secret does not match');
        res.status(401).send('Unauthorized');
        return;
    }

    const receivedData = req.body;

    console.log(`${new Date().toISOString()} - Received webhook data:`, JSON.stringify(receivedData, null, 2));

    res.status(200).send('OK');
});

app.listen(port, () => {
    console.log(`Listening for webhooks on port ${port}...`);
});
