drop table if exists Person;
drop table if exists University;
drop table if exists Paper;
drop table if exists Authorship;

create table University (
    id integer NOT NULL,
    name varchar(40) NOT NULL,
    PRIMARY KEY (id)
);

create table Paper (
    id integer NOT NULL,
    title varchar(40) NOT NULL,
    PRIMARY KEY (id)
);

create table Person (
    id integer NOT NULL,
    name varchar(40) NOT NULL,
    age integer,
    university_id integer,
    supervisor integer,
    PRIMARY KEY (id),
    FOREIGN KEY (university_id) REFERENCES University(id),
    FOREIGN KEY (supervisor) REFERENCES Person(id)
);

create table Authorship (
    paper_id integer NOT NULL,
    author_id integer NOT NULL,
    PRIMARY KEY (paper_id, author_id),
    FOREIGN KEY (paper_id) REFERENCES Paper(id),
    FOREIGN KEY (author_id) REFERENCES Person(id)
);

insert into University values
    (1, 'Stanford'),
    (2, 'MIT');

insert into Person values
    (1, 'Alice',   22, 1, NULL),
    (2, 'Bob',     23, 1, 1),
    (3, 'Charlie', 24, 2, NULL),
    (4, 'Dave',    25, 2, 3),
    (5, 'Eddie',   26, 2, NULL);

insert into Paper values
    (1, 'Effects of XYZ on ABC'),
    (2, 'ABC considered harmful');

insert into Authorship values
    (1, 1),
    (1, 2),
    (2, 2),
    (2, 3);