CREATE TABLE users
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    username   VARCHAR(50)  NOT NULL,
    email      VARCHAR(100) NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email)
VALUES ('alice', 'alice@example.com'),
       ('bob', 'bob@example.com'),
       ('charlie', 'charlie@example.com');

CREATE TABLE orders
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    user_id    INT            NOT NULL,
    product    VARCHAR(100)   NOT NULL,
    amount     DECIMAL(10, 2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
);

INSERT INTO orders (user_id, product, amount)
VALUES (1, 'Book', 19.99),
       (1, 'Pen', 2.49),
       (2, 'Laptop', 999.00),
       (3, 'Coffee', 4.99),
       (3, 'Notebook', 5.49);

SELECT *
FROM playground.users;