{{ config(materialized="view", schema="transformings") }}


select
    e.empid,
    e.lastname,
    e.firstname,
    e.title,
    e.hiredate,
    e.extension,
    iff(m.firstname is NULL, e.firstname,m.firstname) as manager,
    e.yearsalary,
    o.officecity,
    o.officecountry
from {{ ref("staging_empl") }} as e
left join {{ ref("staging_empl") }} as m on e.reportto = m.empid
inner join {{ ref("lookup_o") }} as o on e.office = o.office
