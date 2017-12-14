use bgtraining; 

select 
  distinct 
  name, 
  (collect_set(concat("Device: ",device_type," Count: ", device_count)) over (partition by name order by device_count desc))[0] as most_popular_device,
  (collect_set(concat("Browser: ",browser," Count: ", browser_count)) over (partition by name order by browser_count desc))[0] as most_popular_browser,
  (collect_set(concat("OS: ",operating_system," Count: ", os_count)) over (partition by name order by os_count desc))[0] as most_popular_os
from (
		select
		   distinct
		   c.name,
           l.device_type,
           l.browser, 
           l.operating_system,
		   count(*) over (partition by c.name, l.device_type) as device_count, 
		   count(*) over (partition by c.name, l.browser) as browser_count, 
		   count(*) over (partition by c.name, l.operating_system) as os_count 
		from logs_parsed l
			 left join cities c on c.id = l.cityid
		where c.name is not NULL 
) tmp
;