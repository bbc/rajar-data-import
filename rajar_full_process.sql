insert into rajar.rajar_quarters
select top 1 rw.file_quarter,
             case when rq.file_quarter is null then 'No' else 'Yes' end as check_data,
             min(week_beginning)                                        as quarter_date
from rajar.rajar_weights rw
         inner join radio1_sandbox.af_date_lookup adl on adl.quarter = replace(file_quarter, 'q', ' Q')
         left join rajar.rajar_quarters rq on rw.file_quarter = rq.file_quarter
where check_data = 'No'
group by 1, 2
order by 3;

drop table if exists rajar.replist_temp;
create table rajar.replist_temp as (select distinct file_quarter,
                                                    report_id,
                                                    report_name,
                                                    station_code,
                                                    reporting_period,
                                                    case when report_name ilike ('%bbc%') then 'BBC' else 'Commercial' end as bbc_or_commercial,
                                                    platform_name,
                                                    ooa_type
                                    from rajar.rajar_replist
                                    where file_quarter /*not*/ in
                                          (select top 1 file_quarter
                                           from rajar.rajar_quarters
                                           where in_data in ('No')
                                           order by quarter_date));
grant all on rajar.replist_temp to group rajar_users;

drop table if exists rajar.tsa_temp;
create table rajar.tsa_temp as (select distinct file_quarter, report_id, segment
                                from rajar.rajar_tsa
                                where file_quarter /*not*/ in
                                      (select top 1 file_quarter
                                       from rajar.rajar_quarters
                                       where in_data in ('No')
                                       order by quarter_date));
grant all on rajar.tsa_temp to group rajar_users;

drop table if exists rajar.tsa_replist;
create table rajar.tsa_replist as (select distinct replist.file_quarter, replist.report_id, tsa.segment
                                   from rajar.replist_temp replist
                                            inner join rajar.tsa_temp tsa on replist.report_id = tsa.report_id);
grant all on rajar.tsa_replist to group rajar_users;

drop table if exists rajar.segs_temp;
create table rajar.segs_temp as (select distinct file_quarter, segment, station_code
                                 from rajar.rajar_segs
                                 where file_quarter /*not*/ in
                                       (select top 1 file_quarter
                                        from rajar.rajar_quarters
                                        where in_data in ('No')
                                        order by quarter_date)) );
grant all on rajar.segs_temp to group rajar_users;

drop table if exists rajar.postal_temp;
create table rajar.postal_temp as (select distinct file_quarter, sample_point, segment
                                   from rajar.rajar_postal_sector
                                   where file_quarter /*not*/ in
                                         (select top 1 file_quarter
                                          from rajar.rajar_quarters
                                          where in_data in ('No')
                                          order by quarter_date)) );
grant all on rajar.postal_temp to group rajar_users;

drop table if exists rajar.listening_temp;
create table rajar.listening_temp as (select *
                                      from rajar.rajar_listening
                                      where file_quarter /*not*/ in
                                            (select top 1 file_quarter
                                             from rajar.rajar_quarters
                                             where in_data in ('No')
                                             order by quarter_date)) );
grant all on rajar.listening_temp to group rajar_users;

drop table if exists rajar.individuals_temp;
create table rajar.individuals_temp as (select distinct file_quarter, respid, sample_point, age_15plus, sex, age
                                        from rajar.rajar_individuals
                                        where file_quarter /*not*/ in
                                              (select top 1 file_quarter
                                               from rajar.rajar_quarters
                                               where in_data in ('No')
                                               order by quarter_date)) );
grant all on rajar.individuals_temp to group rajar_users;

drop table if exists rajar.weights_temp;
create table rajar.weights_temp as (select distinct file_quarter, respid, reporting_period, weight
                                    from rajar.rajar_weights
                                    where weight is not null
                                      and file_quarter /*not*/ in
                                          (select top 1 file_quarter
                                           from rajar.rajar_quarters
                                           where in_data in ('No')
                                           order by quarter_date)) );
grant all on rajar.weights_temp to group rajar_users;

drop table if exists rajar.listen_w_ages;
create table rajar.listen_w_ages as (select listening.*,
                                            individuals.sample_point,
                                            individuals.age_15plus,
                                            individuals.age,
                                            individuals.sex
                                     from rajar.listening_temp listening
                                              left join rajar.individuals_temp individuals
                                                        on listening.respid = individuals.respid and
                                                           listening.file_quarter =
                                                           individuals.file_quarter);
grant all on rajar.listen_w_ages to group rajar_users;

drop table if exists rajar.metadata_no_weights;
create table rajar.metadata_no_weights as (select distinct replist.file_quarter,
                                                           replist.report_id,
                                                           replist.report_name,
                                                           replist.station_code,
                                                           replist.reporting_period,
                                                           replist.bbc_or_commercial,
                                                           replist.platform_name,
                                                           segs.segment,
                                                           coalesce(postal_sector.sample_point, postal_sector2.sample_point) as sample_point
                                           from rajar.replist_temp replist
                                                    left join rajar.segs_temp segs
                                                              on replist.station_code =
                                                                 segs.station_code and
                                                                 replist.file_quarter = segs.file_quarter
                                                                  and replist.ooa_type != 1
                                                    left join rajar.tsa_replist
                                                              on replist.report_id = tsa_replist.report_id
                                                                  and tsa_replist.file_quarter = replist.file_quarter
                                                    left join rajar.postal_temp postal_sector
                                                              on (segs.segment = postal_sector.segment and
                                                                  segs.file_quarter = postal_sector.file_quarter) and
                                                                 ooa_type = 2
                                                    left join rajar.postal_temp postal_sector2 on
                                                   (postal_sector2.segment = tsa_replist.segment and
                                                    postal_sector2.file_quarter = tsa_replist.file_quarter) and
                                                   ooa_type = 1);
grant all on rajar.metadata_no_weights to group rajar_users;
drop table if exists rajar.first_summary_temp;
create table rajar.first_summary_temp as (select a.file_quarter,
                                                 a.respid,
                                                 a.dayofweek,
                                                 a.station_code,
                                                 a.location,
                                                 a.code,
                                                 a.sex,
                                                 a.age,
                                                 value,
                                                 a.hhmm_start,
                                                 a.hhmm_end,
                                                 a.n_occasions,
                                                 a.mins,
                                                 a.row_id,
                                                 a.age_15plus,
                                                 report_id,
                                                 b.report_name,
                                                 b.reporting_period,
                                                 b.segment,
                                                 b.sample_point,
                                                 b.bbc_or_commercial,
                                                 b.platform_name,
                                                 c.weight,
                                                 house.bbc_regions,
                                                 house.household_social_grade
                                          from rajar.listen_w_ages a
                                                   left join rajar.metadata_no_weights b
                                                             on a.station_code = b.station_code and
                                                                a.sample_point = b.sample_point and
                                                                b.file_quarter = a.file_quarter
                                                   left join rajar.weights_temp c
                                                             on a.respid = c.respid and
                                                                b.reporting_period = c.reporting_period and
                                                                a.file_quarter = c.file_quarter
                                                   left join rajar.rajar_households house
                                                             on left(a.respid, 16) = house.hhid and
                                                                a.file_quarter = house.file_quarter
                                          where weight is not null;
grant all on rajar.first_summary_temp to group rajar_users;

update rajar.rajar_quarters
set in_data = 'Yes'
where file_quarter = (select distinct file_quarter from rajar.replist_temp);


delete
from rajar.summary_table_1
where quarter in (select distinct file_quarter from rajar.first_summary_temp);
insert into rajar.summary_table_1
with listens as (select fst.file_quarter as quarter,
                        report_name,
                        reporting_period,
                        bbc_or_commercial,
                        age,
                        sex,
                        bbc_regions,
                        household_social_grade,
                        respid,
                        weight,
                        quarter_date,
                        sum(mins) / 60.0 as time_spent
                 from rajar.first_summary_temp fst
                          inner join rajar.rajar_quarters rq on rq.file_quarter = fst.file_quarter
                 where weight > 0
--                                                          and age >= 15
                 group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
-- ALL ADULTS
   , all_adults as (select quarter,
                           'All Adults A15+'                                        as demographic_type,
                           'Adults A15+'                                            as demographic,
                           'A15+'                                                   as age_break,
                           report_name                                              as station,
                           reporting_period,
                           bbc_or_commercial,
                           quarter_date,
                           count(distinct respid)                                   as respondents,
                           sum(weight) * 1000                                       as accounts,
                           sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                           hours / accounts                                         as time_per_account,
                           floor(hours / accounts)                                  as numerical_hours_account,
                           round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                    from listens
                    where age >= 15
                    group by 1, 2, 3, 4, 5, 6, 7, 8
                    union all
                    select quarter,
                           'All Adults A10+'                                        as demographic_type,
                           'Adults A10+'                                            as demographic,
                           'A10+'                                                   as age_break,
                           report_name                                              as station,
                           reporting_period,
                           bbc_or_commercial,
                           quarter_date,
                           count(distinct respid)                                   as respondents,
                           sum(weight) * 1000                                       as accounts,
                           sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                           hours / accounts                                         as time_per_account,
                           floor(hours / accounts)                                  as numerical_hours_account,
                           round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                    from listens
                    where age >= 10
                    group by 1, 2, 3, 4, 5, 6, 7, 8)
-- SPLIT BY AGE
   , age as (select quarter,
                    'Age'                                                    as demographic_type,
                    case
                        when age < 15 then 'U15'
                        when age between 15 and 24
                            then '15-24'
                        when age between 25 and 34
                            then '25-34'
                        when age between 35 and 44
                            then '35-44'
                        when age between 45 and 54
                            then '45-54'
                        when age between 55 and 64
                            then '55-64'
                        when age > 64
                            then '65+' end                                   as demographic,
                    'A15+'                                                   as age_break,
                    report_name                                              as station,
                    reporting_period,
                    bbc_or_commercial,
                    quarter_date,
                    count(distinct respid)                                   as respondents,
                    sum(weight) * 1000                                       as accounts,
                    sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                    hours / accounts                                         as time_per_account,
                    floor(hours / accounts)                                  as numerical_hours_account,
                    round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
             from listens
             where age >= 15
             group by 1, 2, 3, 4, 5, 6, 7, 8
             UNION ALL
             select quarter,
                    'Age'                                                    as demographic_type,
                    case
                        when age < 15 then 'U15'
                        when age between 15 and 24
                            then '15-24'
                        when age between 25 and 34
                            then '25-34'
                        when age between 35 and 44
                            then '35-44'
                        when age between 45 and 54
                            then '45-54'
                        when age between 55 and 64
                            then '55-64'
                        when age > 64
                            then '65+' end                                   as demographic,
                    'A10+'                                                   as age_break,
                    report_name                                              as station,
                    reporting_period,
                    bbc_or_commercial,
                    quarter_date,
                    count(distinct respid)                                   as respondents,
                    sum(weight) * 1000                                       as accounts,
                    sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                    hours / accounts                                         as time_per_account,
                    floor(hours / accounts)                                  as numerical_hours_account,
                    round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
             from listens
             where age >= 10
             group by 1, 2, 3, 4, 5, 6, 7, 8)
-- SPLIT BY GENDER
   , gender as (select quarter,
                       'Gender'                                                 as demographic_type,
                       sex                                                      as demographic,
                       'A15+'                                                   as age_break,
                       report_name                                              as station,
                       reporting_period,
                       bbc_or_commercial,
                       quarter_date,
                       count(distinct respid)                                   as respondents,
                       sum(weight) * 1000                                       as accounts,
                       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                       hours / accounts                                         as time_per_account,
                       floor(hours / accounts)                                  as numerical_hours_account,
                       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                from listens
                where age >= 15
                group by 1, 2, 3, 4, 5, 6, 7, 8
                UNION ALL
                select quarter,
                       'Gender'                                                 as demographic_type,
                       sex                                                      as demographic,
                       'A10+'                                                   as age_break,
                       report_name                                              as station,
                       reporting_period,
                       bbc_or_commercial,
                       quarter_date,
                       count(distinct respid)                                   as respondents,
                       sum(weight) * 1000                                       as accounts,
                       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                       hours / accounts                                         as time_per_account,
                       floor(hours / accounts)                                  as numerical_hours_account,
                       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                from listens
                where age >= 10
                group by 1, 2, 3, 4, 5, 6, 7, 8)
-- SPLIT BY REGION
   , region as (select quarter,
                       'Regions'                                                as demographic_type,
                       bbc_regions                                              as demographic,
                       'A15+'                                                   as age_break,
                       report_name                                              as station,
                       reporting_period,
                       bbc_or_commercial,
                       quarter_date,
                       count(distinct respid)                                   as respondents,
                       sum(weight) * 1000                                       as accounts,
                       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                       hours / accounts                                         as time_per_account,
                       floor(hours / accounts)                                  as numerical_hours_account,
                       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                from listens
                where age >= 15
                group by 1, 2, 3, 4, 5, 6, 7, 8
                UNION ALL
                select quarter,
                       'Regions'                                                as demographic_type,
                       bbc_regions                                              as demographic,
                       'A10+'                                                   as age_break,
                       report_name                                              as station,
                       reporting_period,
                       bbc_or_commercial,
                       quarter_date,
                       count(distinct respid)                                   as respondents,
                       sum(weight) * 1000                                       as accounts,
                       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                       hours / accounts                                         as time_per_account,
                       floor(hours / accounts)                                  as numerical_hours_account,
                       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                from listens
                where age >= 10
                group by 1, 2, 3, 4, 5, 6, 7, 8)
-- SPLIT BY SOCIAL GRADE
   , seg as (select quarter,
                    'Social Grade'                                           as demographic_type,
                    case
                        when household_social_grade in ('A', 'B', 'C1')
                            then 'ABC1'
                        when household_social_grade in ('C2', 'D', 'E')
                            then 'C2DE'
                        else 'Unknown' end                                   as demographic,
                    'A15+'                                                   as age_break,
                    report_name                                              as station,
                    reporting_period,
                    bbc_or_commercial,
                    quarter_date,
                    count(distinct respid)                                   as respondents,
                    sum(weight) * 1000                                       as accounts,
                    sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                    hours / accounts                                         as time_per_account,
                    floor(hours / accounts)                                  as numerical_hours_account,
                    round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
             from listens
             where age >= 15
             group by 1, 2, 3, 4, 5, 6, 7, 8
             UNION ALL
             select quarter,
                    'Social Grade'                                           as demographic_type,
                    case
                        when household_social_grade in ('A', 'B', 'C1')
                            then 'ABC1'
                        when household_social_grade in ('C2', 'D', 'E')
                            then 'C2DE'
                        else 'Unknown' end                                   as demographic,
                    'A10+'                                                   as age_break,
                    report_name                                              as station,
                    reporting_period,
                    bbc_or_commercial,
                    quarter_date,
                    count(distinct respid)                                   as respondents,
                    sum(weight) * 1000                                       as accounts,
                    sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                    hours / accounts                                         as time_per_account,
                    floor(hours / accounts)                                  as numerical_hours_account,
                    round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
             from listens
             where age >= 10
             group by 1, 2, 3, 4, 5, 6, 7, 8)
   , platform_listens as (select fst.file_quarter as quarter,
                                 report_name,
                                 reporting_period,
                                 bbc_or_commercial,
                                 age,
                                 respid,
                                 platform_name,
                                 weight,
                                 quarter_date,
                                 sum(mins) / 60.0 as time_spent
                          from rajar.first_summary_temp fst
                                   inner join rajar.rajar_quarters rq on rq.file_quarter = fst.file_quarter
                          where weight > 0
                                --and age >= 15
                          group by 1, 2, 3, 4, 5, 6, 7, 8, 9)
-- SPLIT BY PLATFORM
   , platform as (select quarter,
                         'Platform'                                               as demographic_type,
                         case
                             when platform_name = 'smartsp'
                                 then 'Smart Speaker'
                             when platform_name = 'amfm'
                                 then 'AM/FM'
                             when platform_name = 'dtv'
                                 then 'DTV'
                             when platform_name = 'online'
                                 then 'Internet'
                             when platform_name = 'dab'
                                 then 'DAB'
                             else 'Unknown'
                             end                                                  as demographic,
                         'A15+'                                                   as age_break,
                         report_name                                              as station,
                         reporting_period,
                         bbc_or_commercial,
                         quarter_date,
                         count(distinct respid)                                   as respondents,
                         sum(weight) * 1000                                       as accounts,
                         sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                         hours / accounts                                         as time_per_account,
                         floor(hours / accounts)                                  as numerical_hours_account,
                         round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                  from platform_listens
                  where age >= 15
                  group by 1, 2, 3, 4, 5, 6, 7, 8
                  UNION ALL
                  select quarter,
                         'Platform'                                               as demographic_type,
                         case
                             when platform_name = 'smartsp'
                                 then 'Smart Speaker'
                             when platform_name = 'amfm'
                                 then 'AM/FM'
                             when platform_name = 'dtv'
                                 then 'DTV'
                             when platform_name = 'online'
                                 then 'Internet'
                             when platform_name = 'dab'
                                 then 'DAB'
                             else 'Unknown'
                             end                                                  as demographic,
                         'A10+'                                                   as age_break,
                         report_name                                              as station,
                         reporting_period,
                         bbc_or_commercial,
                         quarter_date,
                         count(distinct respid)                                   as respondents,
                         sum(weight) * 1000                                       as accounts,
                         sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                         hours / accounts                                         as time_per_account,
                         floor(hours / accounts)                                  as numerical_hours_account,
                         round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                  from platform_listens
                  where age >= 10
                  group by 1, 2, 3, 4, 5, 6, 7, 8
                  UNION ALL
                  select quarter,
                         'Platform'                                               as demographic_type,
                         case
                             when platform_name in ('smartsp', 'dab', 'dtv', 'online') then 'Any Digital'
                             end                                                  as demographic,
                         'A15+'                                                   as age_break,
                         report_name                                              as station,
                         reporting_period,
                         bbc_or_commercial,
                         quarter_date,
                         count(distinct respid)                                   as respondents,
                         sum(weight) * 1000                                       as accounts,
                         sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                         hours / accounts                                         as time_per_account,
                         floor(hours / accounts)                                  as numerical_hours_account,
                         round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                  from platform_listens
                  where age >= 15
                    and demographic is not null
                  group by 1, 2, 3, 4, 5, 6, 7, 8
                  UNION ALL
                  select quarter,
                         'Platform'                                               as demographic_type,
                         case
                             when platform_name in ('smartsp', 'dab', 'dtv', 'online') then 'Any Digital'
                             end                                                  as demographic,
                         'A10+'                                                   as age_break,
                         report_name                                              as station,
                         reporting_period,
                         bbc_or_commercial,
                         quarter_date,
                         count(distinct respid)                                   as respondents,
                         sum(weight) * 1000                                       as accounts,
                         sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
                         hours / accounts                                         as time_per_account,
                         floor(hours / accounts)                                  as numerical_hours_account,
                         round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
                  from platform_listens
                  where age >= 10
                    and demographic is not null
                  group by 1, 2, 3, 4, 5, 6, 7, 8)
select *
from all_adults
union all
select *
from age
union all
select *
from gender
union all
select *
from region
union all
select *
from platform
union all
select *
from seg;

update rajar.summary_table_1
set station = 'BBC Radio 1Xtra'
where station = '1Xtra from the BBC';
update rajar.summary_table_1
set station = 'BBC Radio 5 Sports Extra'
where station = 'BBC Radio 5 live sports extra';

grant all on rajar.summary_table_1 to group rajar_users;
grant all on rajar.summary_table_1 to jasmine_breeze;
grant all on rajar.summary_table_1 to samuel_sanyaolu;
grant all on rajar.summary_table_1 to jonathan_roussot;

delete
from rajar.summary_table_2
where quarter in (select distinct file_quarter from rajar.first_summary_temp);--rajar.replist_temp);
insert into rajar.summary_table_2
with listens as (select fst.file_quarter as quarter,
                        report_name      as station,
                        bbc_or_commercial,
                        reporting_period,
                        respid,
                        dayofweek,
                        hhmm_start,
                        hhmm_end,
                        weight,
                        age,
                        quarter_date,
                        sum(mins) / 60.0 as time_spent
                 from rajar.first_summary_temp fst
                          inner join rajar.rajar_quarters rq on rq.file_quarter = fst.file_quarter
                 where weight > 0
                   and age >= 10
                 group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

select quarter,
       bbc_or_commercial,
       reporting_period,
       station,
       dayofweek,
       hhmm_start + ' - ' + hhmm_end                            as slot,
       'A15+'                                                   as age_break,
       quarter_date,
       sum(weight) * 1000                                       as accounts,
       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
       hours / accounts                                         as time_per_account,
       floor(hours / accounts)                                  as numerical_hours_account,
       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
from listens
where age >= 15
group by 1, 2, 3, 4, 5, 6, 7, 8
UNION ALL
select quarter,
       bbc_or_commercial,
       reporting_period,
       station,
       dayofweek,
       hhmm_start + ' - ' + hhmm_end                            as slot,
       'A10+'                                                   as age_break,
       quarter_date,
       sum(weight) * 1000                                       as accounts,
       sum((time_spent /*/ 60*/) * weight) * 1000               as hours,
       hours / accounts                                         as time_per_account,
       floor(hours / accounts)                                  as numerical_hours_account,
       round((time_per_account - numerical_hours_account) * 60) as numerical_minutes_account
from listens
where age >= 10
group by 1, 2, 3, 4, 5, 6, 7, 8;
grant all on rajar.summary_table_2 to group rajar_users;
grant all on rajar.summary_table_2 to jasmine_breeze;
grant all on rajar.summary_table_2 to samuel_sanyaolu;
grant all on rajar.summary_table_2 to jonathan_roussot;
update rajar.summary_table_2
set station = 'BBC Radio 1Xtra'
where station = '1Xtra from the BBC';
update rajar.summary_table_2
set station = 'BBC Radio 5 Sports Extra'
where station = 'BBC Radio 5 live sports extra';

delete
from rajar.second_summary
where file_quarter in (select distinct file_quarter from rajar.replist_temp);
insert into rajar.second_summary
select a.file_quarter,
       a.respid,
       a.dayofweek,
       a.station_code,
       a.location,
       a.code,
       a.sex,
       a.age,
       value,
       a.hhmm_start,
       case when a.hhmm_end = '28:00' then '04:00' else a.hhmm_end end as hhmm_end,
       a.n_occasions,
       a.mins,
       a.row_id,
       a.age_15plus,
       report_id,
       b.report_name,
       b.reporting_period,
       b.segment,
       b.sample_point,
       b.bbc_or_commercial,
       b.platform_name,
       c.weight,
       house.bbc_regions,
       house.household_social_grade,
       case
           when st.rajar_hour <= en.rajar_hour then st.rajar_id
           else st.id end                                              as optional_start_id,
       case
           when st.rajar_hour <= en.rajar_hour then en.rajar_id
           else en.id end                                              as optional_end_id,
       quarter_date
from rajar.listen_w_ages a
         left join rajar.metadata_no_weights b
                   on a.station_code = b.station_code and
                      a.sample_point = b.sample_point and
                      b.file_quarter = a.file_quarter
         left join rajar.weights_temp c
                   on a.respid = c.respid and
                      b.reporting_period = c.reporting_period and
                      a.file_quarter = c.file_quarter
         left join rajar.rajar_households house
                   on left(a.respid, 16) = house.hhid and
                      a.file_quarter = house.file_quarter
         left join radio1_sandbox.af_rajar_slots st on st.rajar_time = hhmm_start
         left join radio1_sandbox.af_rajar_slots en on en.rajar_time = case
                                                                           when a.hhmm_end = '28:00'
                                                                               then '04:00'
                                                                           else a.hhmm_end end
         inner join rajar.rajar_quarters rq on rq.file_quarter = a.file_quarter
where weight is not null;
grant all on rajar.second_summary to group rajar_users;
grant all on radio1_sandbox.af_rajar_slots to group rajar_users;
grant all on radio1_sandbox.af_rajar_slots to rajar_loader;
grant all on rajar.second_summary to jasmine_breeze;
grant all on rajar.second_summary to samuel_sanyaolu;
grant all on rajar.second_summary to jonathan_roussot;

delete
from rajar.rajar_population
where file_quarter in (select distinct file_quarter from rajar.replist_temp);
insert into rajar.rajar_population
select rw.file_quarter,
       reporting_period,
       age,
       quarter_date,
       count(distinct rw.respid)                as sample_size,
       sum(weight) * 1000                       as weight_total,
       sum(case when age >= 15 then weight end) as weight_a15

from rajar.weights_temp rw
         inner join rajar.individuals_temp ri
                    on ri.respid = rw.respid and ri.file_quarter = rw.file_quarter
         inner join rajar.rajar_quarters rq on rq.file_quarter = rw.file_quarter
group by 1, 2, 3, 4;
grant all on rajar.rajar_population to group rajar_users;
grant all on rajar.rajar_population to rajar_loader;
grant all on rajar.rajar_population to jasmine_breeze;
grant all on rajar.rajar_population to samuel_sanyaolu;
grant all on rajar.rajar_population to jonathan_roussot;



-------------------------------------
--- RUN THIS ON RAJAR DAY ---
-------------------------------------

/*delete
from central_insights_sandbox.af_rajar_table_1
where quarter in (select distinct quarter from rajar.summary_table_1);
insert into central_insights_sandbox.af_rajar_table_1*/
drop table if exists central_insights_sandbox.af_rajar_table_1;
create table central_insights_sandbox.af_rajar_table_1 as (select *
                                                           from rajar.summary_table_1)
;
grant all on central_insights_sandbox.af_rajar_table_1 to group rajar_users;
grant all on central_insights_sandbox.af_rajar_table_1 to jasmine_breeze;
grant all on central_insights_sandbox.af_rajar_table_1 to samuel_sanyaolu;
grant all on central_insights_sandbox.af_rajar_table_1 to jonathan_roussot;
grant all on central_insights_sandbox.af_rajar_table_1 to matthew_byrne;

/*delete
from central_insights_sandbox.af_rajar_table_2
where quarter in (select distinct quarter from rajar.summary_table_2);
insert into central_insights_sandbox.af_rajar_table_2*/
drop table if exists central_insights_sandbox.af_rajar_table_2;
create table central_insights_sandbox.af_rajar_table_2 as (select *
                                                           from rajar.summary_table_2)
;
grant all on central_insights_sandbox.af_rajar_table_2 to group rajar_users;
grant all on central_insights_sandbox.af_rajar_table_2 to jasmine_breeze;
grant all on central_insights_sandbox.af_rajar_table_2 to samuel_sanyaolu;
grant all on central_insights_sandbox.af_rajar_table_2 to jonathan_roussot;
grant all on central_insights_sandbox.af_rajar_table_2 to matthew_byrne;

/*delete
from central_insights_sandbox.rajar_population
where file_quarter in (select distinct file_quarter from rajar.rajar_population);
insert into central_insights_sandbox.rajar_population*/
drop table if exists central_insights_sandbox.rajar_population;
create table central_insights_sandbox.rajar_population as (select *
                                                           from rajar.rajar_population)
;
grant all on central_insights_sandbox.rajar_population to group rajar_users;
grant all on central_insights_sandbox.rajar_population to jasmine_breeze;
grant all on central_insights_sandbox.rajar_population to samuel_sanyaolu;
grant all on central_insights_sandbox.rajar_population to jonathan_roussot;
grant all on central_insights_sandbox.rajar_population to matthew_byrne;
