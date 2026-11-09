# High-Concurrency Ticket Booking Backend (Multi-Queue v2)

Isolated queue architecture for **Concerts**, **Movies**, and **Comedy Shows** using a RabbitMQ Topic Exchange (`ticket_topic_exchange`), Redis Lua atomic locking, and MySQL Optimistic Concurrency Control (OCC).

## Setup & Running
1. `npm install`
2. Configure `.env` from `.env.example` and run `schema.sql`.
3. Start API Server: `npm start`
4. Start Dedicated Workers per Event Type:
   - `npm run worker:concert`
   - `npm run worker:movie`
   - `npm run worker:comedy`
