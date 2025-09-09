--Posting Submission Summary
--https://bountyjobs.domo.com/datacenter/dataflows/87/graph
with submissionds as(
SELECT  * FROM bounty_jobs_enhanced.bountyjobs_sql_submission
),
rejectedsubmissionsandgroupby1 as (
select job_posting_id as job_posting_id_to_drop2,count(distinct submission_id) as "SUBMISSIONS REJECTED" from submissionds
where submission_status in ('Rejected','Rejected by ATS')
group by job_posting_id 
),
openedsubmissionsandgroupby as (
select job_posting_id as job_posting_id_to_drop,count(distinct submission_id) as "SUBMISSIONS OPENED" from submissionds
where submission_status not in ('Unopened','Withdrawn')
group by job_posting_id 
),
submissiondatesandtotal as (
select job_posting_id,
MIN(creation_date) as "SUBMISSIONS FIRST DATE",
MAX(creation_date) as "SUBMISSIONS LAST DATE",
count(distinct submission_id) as "SUBMISSIONS TOTAL" 
from submissionds
group by job_posting_id 
),
unopenedsubmissionsandtotalunopened as (
select job_posting_id as job_posting_id_1,count(distinct submission_id) as "SUBMISSIONS UNOPENED" from submissionds
where submission_status in ('Unopened')
group by job_posting_id 
),
allsubsandunopensubs as (
select * from
submissiondatesandtotal st left outer join unopenedsubmissionsandtotalunopened uost
on st.job_posting_id = uost.job_posting_id_1
),
activeinterviewmapping as (
select *,
case
	when submission_status = 'Final Consideration' then 'Active Interview'
	when submission_status = 'In-Person Interview' then 'Active Interview' 
	when submission_status = 'Agency Phone Interview' then 'Active Interview'
	when submission_status = 'Offer Extended' then 'Active Interview'
	when submission_status = 'Phone Interview' then 'Active Interview'
	when submission_status = 'Sent to Employer' then 'Active Interview'
	else null 
end as "SUBMISSION ACTIVE INTERVIEW CATEGORY"
from submissionds
),
activeinterviwandtotalactiveinterview as (
select job_posting_id,count(distinct submission_id) as "SUBMISSIONS ACTIVE INTERVIEWS" 
from activeinterviewmapping
where "SUBMISSION ACTIVE INTERVIEW CATEGORY" in ('Active Interview')
group by job_posting_id 
),
activesubmissionmapping as (
select *,
case
	when submission_status = 'Final Consideration' then 'Active'
	when submission_status = 'In-Person Interview' then 'Active'
	when submission_status = 'ATS Rejection Appeal' then 'Active'
	when submission_status = 'Agency Phone Interview' then 'Active'
	when submission_status = 'Awaiting Feedback' then 'Active'
	when submission_status = 'Closed - No Hire' then 'Inactive'
	when submission_status = 'Hired' then 'Inactive'
	when submission_status = 'Not Uploaded to ATS' then 'Active'
	when submission_status = 'Offer Extended' then 'Active'
	when submission_status = 'Phone Interview' then 'Active'
	when submission_status = 'Rejected' then 'Inactive'
	when submission_status = 'Rejected by ATS' then 'Inactive'
	when submission_status = 'Sent to Employer' then 'Active'
	when submission_status = 'Undecided' then 'Active'
	when submission_status = 'Unopened' then 'Active'
end as "SUBMISSION STATUS CATEGORY"
from submissionds
),activesub as (
select * from activesubmissionmapping where "SUBMISSION STATUS CATEGORY" in ('Active')
),
activesubandtotalactivesub as (
select job_posting_id as JOB_POSTING_ID_2,count(distinct submission_id) as "SUBMISSIONS ACTIVE"
from activesub 
where "SUBMISSION STATUS CATEGORY" in ('Active')
group by job_posting_id
),
allsubsandactivesubs as (
select * from allsubsandunopensubs asub left outer join activesubandtotalactivesub acsub
on asub.job_posting_id = acsub.JOB_POSTING_ID_2
), 
submissionhistds as (
select subhist.submission_id as submission_id_1,
subhist.creation_date as creation_date_1,
subhist.created_by as created_by_1,
subhist.modified_by as modified_by_1,
subhist.submission_status_history_id ,
subhist.status,
subhist.modified_date as modified_date_1
from bounty_jobs_enhanced.bountyjobs_sql_submission_status_history subhist
),
joinsubmissionandsubmissionsttaushistory as (
select * from submissionds sub left outer join submissionhistds subhist
on sub.submission_id = subhist.submission_id_1
),
interviewmapping as (
select *,
case
	when status = 'Final Consideration' then 'In-Person Interview'
	when status = 'In-Person Interview' then 'In-Person Interview'
	when status = 'Agency Phone Interview' then 'Phone Interview'
	when status = 'Hired' then 'In-Person Interview'
	when status = 'Offer Extended' then 'In-Person Interview'
	when status = 'Phone Interview' then 'Phone Interview'
	when status = 'Sent to Employer' then 'Phone Interview'
	when status = 'Award Bounty' then 'In-Person Interview'
	when status = 'Hired And Paid' then 'In-Person Interview'
	when status = 'Hired Not Rejected' then 'In-Person Interview'
	when status = 'Pending Placement' then 'In-Person Interview'
	else null
end as "SUBMISSION INTERVIEW CATEGORY"
from joinsubmissionandsubmissionsttaushistory jtb
),
interviewsandaddformula as (
select *,
case
	when "SUBMISSION INTERVIEW CATEGORY" = 'Phone Interview' then 1
	else 0
end as "PHONE INTERVIEWS",
case
	when "SUBMISSION INTERVIEW CATEGORY" = 'In-Person Interview' then 1
	else 0
end as "IN PERSON INTERVIEWS"
from interviewmapping
where "SUBMISSION INTERVIEW CATEGORY" in ('In-Person Interview','Phone Interview')
),
totalinterviews as (
select iaf.job_posting_id as job_posting_id_1,
count(distinct iaf.submission_id) as "SUBMISSIONS TOTAL INTERVIEWS",
min(iaf.creation_date) as "EARLIEST INTERVIEW DATE",
sum("PHONE INTERVIEWS") as "PHONE INTERVIEWS",
sum("IN PERSON INTERVIEWS") as "IN PERSON INTERVIEWS"
from interviewsandaddformula iaf
group by 1
),
allinterviewsandactiveinterviewsjoin as (
select
ti.job_posting_id_1 as job_posting_id_1_1,
ai.job_posting_id as job_posting_id_3,
ai."SUBMISSIONS ACTIVE INTERVIEWS",
ti."SUBMISSIONS TOTAL INTERVIEWS",
ti."EARLIEST INTERVIEW DATE",
ti."PHONE INTERVIEWS",
ti."IN PERSON INTERVIEWS"
from totalinterviews ti left outer join   activeinterviwandtotalactiveinterview ai
on  ti.job_posting_id_1 = ai.job_posting_id
),
allsubsandallinterviewsjoin as (
select * 
from allsubsandactivesubs asa left outer join allinterviewsandactiveinterviewsjoin ai
on asa.job_posting_id = ai.job_posting_id_1_1
),
joindata as (
select * 
from allsubsandallinterviewsjoin asai left outer join openedsubmissionsandgroupby osg 
on asai.job_posting_id = osg.job_posting_id_to_drop
),
selectcolumn1 as (
select job_posting_id, 
submission_id,
status,
creation_date_1 as "SUBMISSION STATUS CREATION DATE"
from joinsubmissionandsubmissionsttaushistory jssh
),
assignstatussequence as (
select *,
case
	when status = 'Not Uploaded to ATS' then 1
	when status = 'Rejected by ATS' then 2
	when status = 'ATS Rejection Appeal' then 3
	when status = 'Unopened' then 4
	when status = 'Undecided' then 5
	when status = 'Awaiting Feedback' then 6
	when status = 'Agency Phone Interview' then 7
	when status = 'Sent to Employer' then 8
	when status = 'Phone Interview' then 9
	when status = 'In-Person Interview' then 10
	when status = 'Final Consideration' then 11
	when status = 'Offer Extended' then 12
	when status = 'Rejected' then 13
	when status = 'Withdrawn' then 14
	when status = 'Closed - No Hire' then 15
	when status = 'Hired And Paid' then 16
	when status = 'Hired' then 16
	when status = 'Award Bounty' then 16
	when status = 'Pending Placement' then 16
	when status = 'Hired Not Rejected' then 16
end as "SUBMISSION STATUS SEQUENCE"
from selectcolumn1 
where status in ('Unopened','Not Uploaded to ATS','Undecided','Awaiting Feedback','Agency Phone Interview',
'Sent to Employer','Phone Interview','In-Person Interview','Final Consideration','Offer Extended')
),
filterbyreportablestatus as (
select * ,
       ROW_NUMBER() OVER (
           PARTITION BY JOB_POSTING_ID
           ORDER BY "SUBMISSION STATUS SEQUENCE" DESC, "SUBMISSION STATUS CREATION DATE" DESC
       ) AS "STATUS RANK"
from assignstatussequence
),
selecttoprank as (
select * 
from filterbyreportablestatus 
where "STATUS RANK" = 1
),
joinhistorytoactivesubmissions as (
select *
from activesub left outer join submissionhistds
on activesub.submission_id = submissionhistds.submission_id_1
),
filterrowsandassignactivestatussequence as (
select *,
case
	when status = 'Not Uploaded to ATS' then 1
	when status = 'Rejected by ATS' then 2
	when status = 'ATS Rejection Appeal' then 3
	when status = 'Unopened' then 4
	when status = 'Undecided' then 5
	when status = 'Awaiting Feedback' then 6
	when status = 'Agency Phone Interview' then 7
	when status = 'Sent to Employer' then 8
	when status = 'Phone Interview' then 9
	when status = 'In-Person Interview' then 10
	when status = 'Final Consideration' then 11
	when status = 'Offer Extended' then 12
	when status = 'Rejected' then 13
	when status = 'Withdrawn' then 14
	when status = 'Closed - No Hire' then 15
	when status = 'Hired And Paid' then 16
	when status = 'Hired' then 16
	when status = 'Award Bounty' then 16
	when status = 'Pending Placement' then 16
	when status = 'Hired Not Rejected' then 16
end as "SUBMISSION STATUS SEQUENCE"
from joinhistorytoactivesubmissions 
where submission_status = status
),
selectcolumn as (
select submission_id as SUBMISSION_ID_1,
candidate_id,
job_posting_id as JOB_POSTING_ID_1,
name,
email,
recruiter_person_id,
title,
creation_date,
created_by,
modified_by,
"SUBMISSION STATUS SEQUENCE" as "SUBMISSION STATUS SEQUENCE_1",
"SUBMISSION STATUS CATEGORY",
status as "CURRENT_SUBMISSION_STATUS",
creation_date_1 as "CURRENT_SUBMISSION_STATUS_DATE"
from filterrowsandassignactivestatussequence
where status in ('Unopened','Not Uploaded to ATS','Undecided','Awaiting Feedback','Agency Phone Interview',
'Sent to Employer','Phone Interview','In-Person Interview','Final Consideration','Offer Extended')
),
rankactive as (
select * ,
       ROW_NUMBER() OVER (
           PARTITION BY JOB_POSTING_ID_1
           ORDER BY "SUBMISSION STATUS SEQUENCE_1" DESC, "CURRENT_SUBMISSION_STATUS_DATE" DESC
       ) AS "STATUS RANK_1"
from selectcolumn
),
selectactivetoprank as (
select * 
from rankactive 
where "STATUS RANK_1" = 1
),
allfurtheststatuswithcurrentfurtheststatus as (
select * 
from selecttoprank stp left outer join selectactivetoprank satr 
on stp.job_posting_id = satr.JOB_POSTING_ID_1
),
selectcolumns2 as (
select 
job_posting_id as job_posting_id_4, 
"CURRENT_SUBMISSION_STATUS" as "CURRENT FURTHEST SUBMISSION STATUS",
"CURRENT_SUBMISSION_STATUS_DATE" as "CURRENT FURTHEST SUBMISSION STATUS DATE",
Status as "FURTHEST SUBMISSION STATUS",
"SUBMISSION STATUS CREATION DATE" as "FURTHEST SUBMISSION STATUS DATE"
from allfurtheststatuswithcurrentfurtheststatus
),
joinallsubswithallfurtheststatus as (
select * 
from joindata jd left outer join selectcolumns2 sc2
on jd.job_posting_id = sc2.job_posting_id_4
),
joindata1 as (
select * 
from joinallsubswithallfurtheststatus asws left outer join rejectedsubmissionsandgroupby1 rssg 
on asws.job_posting_id = rssg.job_posting_id_to_drop2
),
addformula1 as (
select * ,
 COALESCE("SUBMISSIONS TOTAL INTERVIEWS"::float / NULLIF("SUBMISSIONS TOTAL", 0), 0) AS "INTERVIEW RATE"
 from joindata1
),
selectcolumns as (
select
job_posting_id,
"SUBMISSIONS FIRST DATE",
"SUBMISSIONS LAST DATE",
"SUBMISSIONS TOTAL",
"SUBMISSIONS UNOPENED",
"SUBMISSIONS ACTIVE INTERVIEWS",
"SUBMISSIONS TOTAL INTERVIEWS",
"SUBMISSIONS ACTIVE",
"CURRENT FURTHEST SUBMISSION STATUS",
"CURRENT FURTHEST SUBMISSION STATUS DATE",
"FURTHEST SUBMISSION STATUS",
"FURTHEST SUBMISSION STATUS DATE",
"EARLIEST INTERVIEW DATE",
"PHONE INTERVIEWS",
"IN PERSON INTERVIEWS",
round("INTERVIEW RATE",2) as "INTERVIEW RATE",
"SUBMISSIONS OPENED",
"SUBMISSIONS REJECTED"
from addformula1
)
select * from selectcolumns