

select
	[location],
	datefromparts(year(test_date), month(test_date), 1) [date],
	email_md5,
	count(*) total
into EmailsPerLocationAndMonth
from testcases
where email_md5 is not null
group by [location], DATEFROMPARTS(year(test_date), month(test_date), 1), email_md5
having count(*) > 5
