--1. Find all carriers who cancelled more than 1 flights during 2007, 
-- order them from biggest to lowest by number 
-- of cancelled flights and list in each record all departure cities where cancellation happened. 

select UniqueCarrier, cancelled_flyghts_count, cities_list 
	from ( select   UniqueCarrier, count(*) as cancelled_flyghts_count, concat_ws(",", collect_set(city)) as cities_list
			from  (  select f.UniqueCarrier, a.city from flying f inner join airports a 
					        on a.iata = f.dest and f.cancelled = 1 ) tmp
	group by UniqueCarrier
 ) tmp
order by cancelled_flyghts_count desc;

--2. Based on the explain statement for the query - 3 map reducers are running (see the screen 2)