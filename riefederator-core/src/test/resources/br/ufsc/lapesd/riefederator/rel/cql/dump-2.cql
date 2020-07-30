CREATE KEYSPACE def_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE def_ks;

DROP TABLE IF EXISTS Person;
DROP TABLE IF EXISTS University;
DROP TABLE IF EXISTS Paper;
DROP TABLE IF EXISTS Authorship;

CREATE TABLE University (
    id INT,
    name VARCHAR,
    PRIMARY KEY (id)
);

CREATE TABLE Paper (
    id INT,
    title VARCHAR,
    PRIMARY KEY (id)
);

CREATE TABLE Person (
    id INT,
    name VARCHAR,
    age INT,
    university_id INT,
    supervisor INT,
    PRIMARY KEY (id)
);

CREATE TABLE Authorship (
    paper_id INT,
    author_id INT,
    PRIMARY KEY (paper_id, author_id)
);

INSERT INTO University (id, name) VALUES (1, 'Stanford');
INSERT INTO University (id, name) VALUES (2, 'MIT');

INSERT INTO Person (id, name, age, university_id, supervisor) VALUES (1, 'Alice',   22, 1, NULL);
INSERT INTO Person (id, name, age, university_id, supervisor) VALUES (2, 'Bob',     23, 1, 1);
INSERT INTO Person (id, name, age, university_id, supervisor) VALUES (3, 'Charlie', 24, 2, NULL);
INSERT INTO Person (id, name, age, university_id, supervisor) VALUES (4, 'Dave',    25, 2, 3);
INSERT INTO Person (id, name, age, university_id, supervisor) VALUES (5, 'Eddie',   26, 2, NULL);

INSERT INTO Paper (id, title) VALUES (1, 'Effects of XYZ on ABC');
INSERT INTO Paper (id, title) VALUES (2, 'ABC considered harmful');

INSERT INTO Authorship (paper_id, author_id) VALUES (1, 1);
INSERT INTO Authorship (paper_id, author_id) VALUES (1, 2);
INSERT INTO Authorship (paper_id, author_id) VALUES (2, 2);
INSERT INTO Authorship (paper_id, author_id) VALUES (2, 3);