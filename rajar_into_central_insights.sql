-------------------------------------
--- RUN THIS ON RAJAR DAY ---
-------------------------------------

delete
from central_insights_sandbox.af_rajar_table_1
where quarter in (select distinct quarter from rajar.summary_table_1);
insert into central_insights_sandbox.af_rajar_table_1
select * from rajar.summary_table_1
;
grant all on central_insights_sandbox.af_rajar_table_1 to group rajar_users;
grant all on central_insights_sandbox.af_rajar_table_1 to jasmine_breeze;
grant all on central_insights_sandbox.af_rajar_table_1 to samuel_sanyaolu;
grant all on central_insights_sandbox.af_rajar_table_1 to jonathan_roussot;
grant all on central_insights_sandbox.af_rajar_table_1 to matthew_byrne;
grant all on central_insights_sandbox.af_rajar_table_1 to audiences_reporting_analytics;



delete
from central_insights_sandbox.af_rajar_table_2
where quarter in (select distinct quarter from rajar.summary_table_2);
insert into central_insights_sandbox.af_rajar_table_2
select *
                                                           from rajar.summary_table_2
;
grant all on central_insights_sandbox.af_rajar_table_2 to group rajar_users;
grant all on central_insights_sandbox.af_rajar_table_2 to jasmine_breeze;
grant all on central_insights_sandbox.af_rajar_table_2 to samuel_sanyaolu;
grant all on central_insights_sandbox.af_rajar_table_2 to jonathan_roussot;
grant all on central_insights_sandbox.af_rajar_table_2 to matthew_byrne;
grant all on central_insights_sandbox.af_rajar_table_1 to audiences_reporting_analytics;

delete
from central_insights_sandbox.rajar_population
where file_quarter in (select distinct file_quarter from rajar.rajar_population);
insert into central_insights_sandbox.rajar_population
select * from rajar.rajar_population
;
grant all on central_insights_sandbox.rajar_population to group rajar_users;
grant all on central_insights_sandbox.rajar_population to jasmine_breeze;
grant all on central_insights_sandbox.rajar_population to samuel_sanyaolu;
grant all on central_insights_sandbox.rajar_population to jonathan_roussot;
grant all on central_insights_sandbox.rajar_population to matthew_byrne;
grant all on central_insights_sandbox.af_rajar_table_1 to audiences_reporting_analytics;
;
