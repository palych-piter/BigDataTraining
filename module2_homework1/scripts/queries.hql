--1. Count total number of flights per carrier in 2007 (Screanshot#1)
select UniqueCarrier as carrier, count(*) as count from flying where year = '2007' group by UniqueCarrier;


--2. The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports)(Screanshot#2)
select sum(flying_count) from 
(
select count(*) as flying_count from flying f
   inner join airports a on a.iata = f.origin and a.city = 'New York' and f.Month = 6
union all 
select count(*) as flying_count from flying f
   inner join airports a on a.iata = f.dest and a.city = 'New York' and f.Month = 6

) tmp;



--3. Find five most busy airports in US during Jun 01 - Aug 31. (Screanshot#3)
set mapred.reduce.tasks = 1;
select airport, flying_number from ( 

	  select distinct airport, SUM(count) OVER (PARTITION BY airport) as flying_number FROM (

		  select distinct dest as airport, COUNT(dest) OVER (PARTITION BY dest) as count FROM flying f 
		  inner join airports a 
			      on a.iata = f.dest and a.country = 'USA' and f.Month >= 6 and f.Month <= 8

		  union all
		  select distinct origin as airport, COUNT(origin) OVER (PARTITION BY origin) as count FROM flying f
		  inner join airports a 
			      on a.iata = f.origin and a.country = 'USA' and f.Month >= 6 and f.Month <= 8	  
	  
	  ) tmp 
  
  ) tmp1

order by flying_number desc limit 5;


-- 4. Find the carrier who served the biggest number of flights (Screanshot#4)
set mapred.reduce.tasks = 1;
select carrier, count from (

select distinct UniqueCarrier as carrier, COUNT(UniqueCarrier) OVER (PARTITION BY UniqueCarrier) as count 
  from flying 

) tmp 
order by count desc limit 1;

