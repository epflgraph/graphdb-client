-- Data fixture for GraphDB deployment matrix tests.
INSERT INTO users (user_id, email, display_name, active) VALUES
('u001', 'alice@example.com', 'Alice', 1),
('u002', 'bob@example.com', 'Bob', 1),
('u003', 'charlie@example.com', 'Charlie', 0),
('u004', 'diana@example.com', 'Diana', 1);

INSERT INTO posts (post_id, author_user_id, title, body) VALUES
('p001', 'u001', 'First post', 'Hello world'),
('p002', 'u001', 'Second post', 'More content'),
('p003', 'u002', 'Bobs post', 'Bob says hi'),
('p004', 'u004', 'Dianas post', 'Diana writes');
