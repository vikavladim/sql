create database project00;

create table cities(point1 text,point2 text,cost int);

insert into cities values
('a','b',10),
('a','d',20),
('a','c',15),
('b','d',25),
('b','c',35),
('c','d',30),
('b','a',10),
('d','a',20),
('c','a',15),
('d','b',25),
('c','b',35),
('d','c',30);


with recursive routs(total_cost, prev, tour) as
(
select 0,'a', array['a'] as tour
union all
select
routs.total_cost+cities.cost,
point2,
tour || point2
from routs
join cities on prev=point1
where (point2 <> all(tour) OR point2 = 'a' ) AND
cardinality(array_positions(tour, 'a')) < 2
), full_routs AS
(
select * from routs WHERE cardinality(tour) = 5
)

SELECT total_cost, tour
FROM full_routs
WHERE total_cost = (SELECT MIN(total_cost) FROM full_routs) 
ORDER BY total_cost,tour;