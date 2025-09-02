with attributions as (
  -- Route 1: Learners via classroom → site → partner
  select
    null as educator_id,
    eclm.user_id as learner_id,
    ec.site_id,
    us.name as site_name,
    up.id as partner_id,
    up.name as partner_name,
    upic.code as partner_code,
    ec.id as classroom_id,
    ec.name as classroom_name,
    ecic.code as classroom_code
  from {{ source('raw','educator_classroomlearnermembership') }} as eclm
  left join {{ source('raw','educator_classroom') }} as ec on ec.id = eclm.classroom_id
  left join {{ source('raw','user_site') }} as us on ec.site_id = us.id
  left join {{ source('raw','user_partner') }} as up on us.partner_id = up.id
  left join {{ source('raw','user_partnerinvitecode') }} as upic on upic.partner_id = up.id
  left join {{ source('raw','educator_classroominvitecode') }} as ecic on ecic.classroom_id = ec.id

  union all

  -- Route 2: Educators via classroom → site → partner
  select
    ece.user_id as educator_id,
    null as learner_id,
    ec.site_id,
    us.name as site_name,
    up.id as partner_id,
    up.name as partner_name,
    upic.code as partner_code,
    ec.id as classroom_id,
    ec.name as classroom_name,
    ecic.code as classroom_code
  from {{ source('raw','educator_classroom_educators') }} as ece
  left join {{ source('raw','educator_classroom') }} as ec on ece.classroom_id = ec.id
  left join {{ source('raw','user_site') }} as us on ec.site_id = us.id
  left join {{ source('raw','user_partner') }} as up on us.partner_id = up.id
  left join {{ source('raw','user_partnerinvitecode') }} as upic on upic.partner_id = up.id
  left join {{ source('raw','educator_classroominvitecode') }} as ecic on ecic.classroom_id = ec.id

  union all

  -- Route 3: Learners invited via classroom invitation (matched by email)
  select
    null as educator_id,
    uu.id as learner_id,
    ec.site_id,
    us.name as site_name,
    up.id as partner_id,
    up.name as partner_name,
    upic.code as partner_code,
    ec.id as classroom_id,
    ec.name as classroom_name,
    ecic.code as classroom_code
  from {{ source('raw','educator_classroominvitation') }} as eci
  join {{ source('raw','user_user') }} as uu on lower(trim(uu.email)) = lower(trim(eci.email)) and uu.type != 'IL'
  join {{ source('raw','educator_classroom') }} as ec on ec.id = eci.classroom_id
  left join {{ source('raw','user_site') }} as us on ec.site_id = us.id
  left join {{ source('raw','user_partner') }} as up on us.partner_id = up.id
  left join {{ source('raw','user_partnerinvitecode') }} as upic on upic.partner_id = up.id
  left join {{ source('raw','educator_classroominvitecode') }} as ecic on ecic.classroom_id = ec.id

  union all

  -- Route 4: Learners who joined via partner invite code
  select
    null as educator_id,
    uu.id as learner_id,
    upic.site_id,
    us.name as site_name,
    up.id as partner_id,
    up.name as partner_name,
    upic.code as partner_code,
    null as classroom_id,
    null as classroom_name,
    null as classroom_code
  from {{ source('raw','action_userjoinsaction') }} as a
  join {{ source('raw','user_user') }} as uu on uu.id = a.user_id and uu.type != 'IL'
  join {{ source('raw','user_partnerinvitecode') }} as upic on a.partner_invite_code_id = upic.id
  left join {{ source('raw','user_partner') }} as up on up.id = upic.partner_id
  left join {{ source('raw','user_site') }} as us on upic.site_id = us.id
  where a.action_type = 'userjoins'
)
, stacked_users_partners as (
  select
    educator_id as user_id,
    partner_id,
    partner_name,
    partner_code,
    classroom_id,
    classroom_name,
    classroom_code,
    site_id,
    site_name
  from attributions
  where educator_id is not null
  group by 1,2,3,4,5,6,7,8,9

  union all

  select
    learner_id as user_id,
    partner_id,
    partner_name,
    partner_code,
    classroom_id,
    classroom_name,
    classroom_code,
    site_id,
    site_name
  from attributions
  where learner_id is not null
  group by 1,2,3,4,5,6,7,8,9
)

select *
from stacked_users_partners
