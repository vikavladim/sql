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
WHERE total_cost = (SELECT MIN(total_cost) FROM full_routs) OR
total_cost = (SELECT MAX(total_cost) FROM full_routs)
ORDER BY total_cost,tour;