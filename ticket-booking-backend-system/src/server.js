const express = require('express');
const http = require('http');
require('dotenv').config();

const { connectRabbitMQ } = require('./config/rabbitmq');
const bookingRoutes = require('./routes/bookingRoutes');

const app = express();
const server = http.createServer(app);

app.use(express.json());

app.use('/api/book', bookingRoutes);

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'UP', timestamp: new Date() });
});

const PORT = process.env.PORT || 3000;

async function startServer() {
    await connectRabbitMQ();
    server.listen(PORT, () => {
        console.log(`Server running on port ${PORT}`);
    });
}

startServer();
