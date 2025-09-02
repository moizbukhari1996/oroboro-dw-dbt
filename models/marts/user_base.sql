with users as (
  select
    uu.id as user_id,
    uu.uuid,
    uu.first_name,
    uu.last_name,
    uu.email,
    uu.type as user_type,
    case when widget.id is not null then 'Widget Account'
         when uu.type = 'E' then 'Advisors'
         when uu.type = 'CL' then 'Cohort Learners'
         when uu.type = 'IL' then 'Independent Learners' end as user_type_full_name,
    case when regexp_replace(lower(trim(uu.first_name)), r'\s+', '') like '%test%'
           or regexp_replace(lower(trim(uu.last_name)), r'\s+', '') like '%test%'
           or regexp_replace(lower(trim(uu.email)), r'\s+', '') like '%test%'
           or uu.email like 'educatorst1@example.com' then true else false end as is_test_user,
    uu.race_ethnicity as race_ethnicity,
    case
      when lower(race_ethnicity) like '%prefer not to say%' or race_ethnicity is null then 'Prefer Not To Say'
      when ( (case when race_ethnicity like '%White%' then 1 else 0 end)
           + (case when race_ethnicity like '%Hispanic%' or race_ethnicity like '%Latinx%' then 1 else 0 end)
           + (case when race_ethnicity like '%Black%' or race_ethnicity like '%African American%' then 1 else 0 end)
           + (case when race_ethnicity like '%South Asian%' or race_ethnicity like '%East Asian%' then 1 else 0 end)
           + (case when race_ethnicity like '%Native Hawaiian or other Pacific Islander%' then 1 else 0 end)
           + (case when race_ethnicity like '%Native American or Alaska Native' then 1 else 0 end)
           + (case when race_ethnicity like '%Other%' then 1 else 0 end) ) > 1 then 'Multiracial'
      when race_ethnicity like 'Hispanic or Latinx' then 'Hispanic'
      when race_ethnicity like 'Black or African American' then 'Black'
      when race_ethnicity like '%South Asian%' or race_ethnicity like '%East Asian%' then 'Asian'
      when race_ethnicity like 'Native Hawaiian or other Pacific Islander' then 'Native Hawaiian or other Pacific Islander'
      when race_ethnicity like 'Native American or Alaska Native' then 'Native American or Alaska Native'
      when race_ethnicity like 'White' then 'White'
      when race_ethnicity like 'Other' then 'Other'
      else 'Other' end as race,
    uu.gender,
    uu.self_describe_gender,
    case
      when uu.gender like '%Prefer not to say%' or uu.gender is null then 'Prefer Not To Say'
      when uu.gender like '%Prefer to self-describe%' then 'Prefer Not To Say'
      when uu.gender like '%Man%' and uu.gender like '%Woman%' then 'Prefer Not To Say'
      when uu.gender like '%Man%' then 'Man'
      when uu.gender like '%Woman%' then 'Woman'
      else 'Non-binary' end as gender_sum,
    uu.date_joined,
    uu.is_active,
    case when uu.is_active = false then 'deactivated' else 'active' end as account_status,
    uu.is_staff,
    case
      when uu.birthday is null then null
      else date_diff(
        current_date,
        SAFE.PARSE_DATE('%Y-%m-%d', concat(substr(uu.birthday, 4, 4), '-', substr(uu.birthday, 1, 2), '-01')),
        year
      ) - if(format_date('%m%d', current_date) < concat(substr(uu.birthday, 1, 2), '01'), 1, 0) end as age,
    uu.location_id
  from {{ source('raw','user_user') }} as uu
  left join {{ source('raw','widget_widgetuserapikey') }} as widget on widget.user_id = uu.id
)

select
  users.*,
  coalesce(loc.country, 'Prefer Not To Say') as country,
  coalesce(loc.state, 'Prefer Not To Say') as state,
  coalesce(loc.county, 'Prefer Not To Say') as county,
  coalesce(loc.city, 'Prefer Not To Say') as city,
  loc.city_latitude,
  loc.city_longitude,
  upf.partner_id,
  case
    when upf.partner_name is null and users.user_type = 'CL' then 'No Partner associated with this account'
    when upf.partner_name is null and users.user_type = 'E' then 'No Partner associated with this account'
    when users.user_type = 'IL' then null
    else upf.partner_name end as partner_name,
  upf.partner_code,
  upf.classroom_id,
  upf.classroom_name,
  upf.classroom_code,
  upf.site_id,
  upf.site_name
from users
left join {{ ref('stacked_users_partners') }} as upf on users.user_id = upf.user_id
left join {{ ref('locations_clean') }} as loc on users.location_id = loc.from_location_id
order by users.user_id, upf.partner_id, upf.classroom_id, upf.site_id asc
