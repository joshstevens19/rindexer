const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port = 5003;

// Middleware to parse JSON bodies
app.use(bodyParser.json());

// Middleware to parse URL-encoded bodies
app.use(bodyParser.urlencoded({ extended: true }));

// Webhook endpoint
app.post('/webhook', (req, res) => {
    const receivedData = req.body;

    console.log(`${new Date().toISOString()} - Received webhook data:`, receivedData);

    // Respond with a 200 status and "OK" message
    res.status(200).send('OK');
});

// Start the server
app.listen(port, () => {
    console.log(`Listening for webhooks on port ${port}...`);
});
