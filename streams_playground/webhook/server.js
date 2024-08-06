const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port = 5003;

app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: true }));

app.post('/webhook', (req, res) => {
    const receivedData = req.body;

    console.log(`${new Date().toISOString()} - Received webhook data:`, receivedData);

    res.status(200).send('OK');
});

app.listen(port, () => {
    console.log(`Listening for webhooks on port ${port}...`);
});
