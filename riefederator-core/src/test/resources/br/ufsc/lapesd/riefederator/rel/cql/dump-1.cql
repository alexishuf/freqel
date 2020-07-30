CREATE KEYSPACE other_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE other_ks;

DROP TABLE IF EXISTS OtherPerson;

CREATE TABLE OtherPerson (
    id INT PRIMARY KEY,
    name VARCHAR,
    age INT,
    university VARCHAR
);

INSERT  INTO OtherPerson (id, name, age, university) VALUES (1, 'Alice',   22, 'Stanford');
INSERT  INTO OtherPerson (id, name, age, university) VALUES (2, 'Bob',     23, 'Stanford');
INSERT  INTO OtherPerson (id, name, age, university) VALUES (3, 'Charlie', 24, 'MIT');
INSERT  INTO OtherPerson (id, name, age, university) VALUES (4, 'Dave',    25, 'MIT');
INSERT  INTO OtherPerson (id, name, age, university) VALUES (5, 'Eddie',   26, 'MIT');