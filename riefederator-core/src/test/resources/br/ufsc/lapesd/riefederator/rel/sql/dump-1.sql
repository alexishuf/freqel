drop table if exists Person;

create table Person (
    id integer NOT NULL,
    name varchar(40) NOT NULL,
    age integer,
    university varchar(40),
    PRIMARY KEY (id)
);

insert into Person values
    (1, 'Alice',   22, 'Stanford'),
    (2, 'Bob',     23, 'Stanford'),
    (3, 'Charlie', 24, 'MIT'),
    (4, 'Dave',    25, 'MIT'),
    (5, 'Eddie',   26, 'MIT');