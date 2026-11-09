CREATE DATABASE IF NOT EXISTS ticket_booking;
USE ticket_booking;

CREATE TABLE IF NOT EXISTS seats (
    seat_id VARCHAR(50) PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    event_type ENUM('concert', 'movie', 'comedy') NOT NULL,
    status ENUM('AVAILABLE', 'LOCKED', 'SOLD') DEFAULT 'AVAILABLE',
    user_id VARCHAR(50) NULL,
    version INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    payment_intent_id VARCHAR(100) UNIQUE NOT NULL,
    seat_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    event_type ENUM('concert', 'movie', 'comedy') NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
