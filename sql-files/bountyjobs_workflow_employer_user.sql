--https://bountyjobs.domo.com/datacenter/dataflows/65/graph
--work flow : Employer Users :9
with employeruserds as (
	select
		party_id AS "EMPLOYER USER ID",
		creation_date AS "EMPLOYER USER CREATION DATE",
		first_names AS "EMPLOYER USER FIRST NAME",
		last_name AS "EMPLOYER USER LAST NAME",
		user_status AS "EMPLOYER USER STATUS",
		job_title AS "EMPLOYER USER JOB TITLE",
		business_unit AS "EMPLOYER USER BUSINESS UNIT",
		company_id AS "EMPLOYER_COMPANY_ID",
		company_id AS "EMPLOYER COMPANY ID",
		email_address AS "EMPLOYER USER EMAIL ADDRESS",
		phone AS "EMPLOYER USER PHONE",
		last_login AS "EMPLOYER USER LAST LOGIN",
		compensation_owner_name AS "ACCOUNT DIRECTOR",
		compensation_owner_email  AS "ACCOUNT DIRECTOR EMAIL",
		account_manager_name  AS "ACCOUNT MANAGER",
		account_manager_email AS "ACCOUNT MANAGER EMAIL",
		user_handler_name as USER_HANDLER_NAME,
		company_size AS "EMPLOYER COMPANY SIZE",
		"User Type" AS "USER TYPE",
		profile_complete  AS "PROFILE COMPLETE",
		modified_date AS "EMPLOYER USER LAST MODIFIED DATE",
		address_1 AS "EMPLOYER USER ADDRESS 1",
		address_2 AS "EMPLOYER USER ADDRESS 2",
		city AS "EMPLOYER USER CITY",
		state AS "EMPLOYER USER STATE",
		postal_code AS "EMPLOYER USER POSTAL CODE",
		country AS "EMPLOYER USER COUNTRY" ,
		email_confirmed as "EMPLOYER USER EMAIL CONFIRMED",
		(first_names || ' ' || last_name) as "FULL NAME"
	from dbt_noumanjilani.employer_user ac
), 
employerwf as (
	with employer_company as (
select
	C.COMPANY_ID,
	C.NAME,
	C.DESCRIPTION,
	(CASE WHEN C.EXTEND_CREDIT_FLAG = 'Y' THEN TRUE ELSE FALSE END) AS EXTEND_CREDIT,
	C.CREATION_DATE,
	C.CREATED_BY,
	C.MODIFIED_BY,
	C.MODIFIED_DATE,
	C.COMPANY_WEBSITE,
	C.PAYMENT_TERM,
	C.LEGAL_COMPANY_NAME,
	C.PARENT_ID AS COMPANY_PARENT_ID,
	(CASE WHEN C.CUSTOM_CONTRACT_FLAG = 'Y' THEN TRUE ELSE FALSE END) AS CUSTOM_CONTRACT,
	C.LAUNCH_DATE,
	C.POSTING_ALLOWED,
	C.POSTING_ALLOWED_CONTINGENT,
	(CASE WHEN C.RETAINED_ALLOWED_FLAG = 'Y' THEN TRUE ELSE FALSE END) AS RETAINED_ALLOWED,
	(CASE WHEN C.COMPENSATION_HISTORY_ALLOWED = 'Y' THEN TRUE ELSE FALSE END) AS COMPENSATION_HISTORY_ALLOWED,
	C.WORKFLOW_HANDLER_NAME AS COMPANY_HANDLER,
	AC.NAME AS ATS_VENDOR,
	C.INTEGRATION_SUPPRESSION,
	C.COMP_OWNER_ID,
	C.ACT_MGR_ID
FROM 
	bounty_jobs.COMPANY C
	LEFT JOIN bounty_jobs.ATS A ON A.COMPANY_ID = C.COMPANY_ID
	LEFT JOIN bounty_jobs.ATS_COMPANY AC ON A.ATS_COMPANY_ID = AC.ATS_COMPANY_ID
WHERE 
	C.COMPANY_TYPE_CODE = 'E'
	),
	job_posting as (
	SELECT
    JP.JOB_POSTING_ID,
    JP.POSTING_NUMBER AS BOUNTYJOBS_ID,
    JP.COMPANY_ID,
    COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE) AS CREATION_DATE
    FROM bounty_jobs.job_posting JP
    JOIN bounty_jobs.posting_status JPS ON JPS.CODE = JP.POSTING_STATUS_CODE AND JPS.CODE NOT IN ('DR', 'DD', 'TD')
	)
	select 
	ec.COMPANY_ID as "EMPLOYER ID", 
	ec.NAME as "EMPLOYER NAME", 
	ec.COMPANY_HANDLER as "EMPLOYER CUSTOMER TYPE", 
	ec.CREATION_DATE as "EMPLOYER CREATION DATE", 
	ec.LAUNCH_DATE as "EMPLOYER LAUNCH DATE",
	ec.ATS_VENDOR as "EMPLOYER ATS VENDOR",
	null as "EMPLOYER POSTING ALLOWED",
	MIN(jp.creation_date) as "Employer First Post Date",
	max(jp.creation_date) as "Employer Last Post Date"
	from employer_company ec left join job_posting jp on ec.company_id = jp.company_id
	group by ec.COMPANY_ID, 
	ec.NAME, 
	ec.COMPANY_HANDLER, 
	ec.CREATION_DATE, 
	ec.LAUNCH_DATE, 
	ec.ATS_VENDOR
),
joindata1ndselectdata1 as (
	select 
		comc."EMPLOYER USER ID",
		comc."EMPLOYER_COMPANY_ID",
		"EMPLOYER USER CREATION DATE",
		"EMPLOYER USER STATUS",
		"EMPLOYER USER JOB TITLE",
		"EMPLOYER USER BUSINESS UNIT",
		comc."EMPLOYER COMPANY ID",
		"EMPLOYER USER EMAIL ADDRESS",
		"EMPLOYER USER PHONE",
		"EMPLOYER USER LAST LOGIN",
		"FULL NAME",
		"EMPLOYER ID",
		"EMPLOYER NAME",
		"EMPLOYER CUSTOMER TYPE",
		"EMPLOYER CREATION DATE",
		"EMPLOYER LAUNCH DATE",
--        "EMPLOYER LAST USER LOGIN",
		"EMPLOYER FIRST POST DATE",
		"EMPLOYER LAST POST DATE",
		"EMPLOYER ATS VENDOR",
		"EMPLOYER POSTING ALLOWED",
		"EMPLOYER USER FIRST NAME",
		"EMPLOYER USER LAST NAME",
		comc."ACCOUNT DIRECTOR",
		"ACCOUNT DIRECTOR EMAIL",
		comc."ACCOUNT MANAGER",
		"ACCOUNT MANAGER EMAIL",
		"USER_HANDLER_NAME",
		"EMPLOYER COMPANY SIZE",
		"USER TYPE",
		"EMPLOYER USER LAST MODIFIED DATE",
		"EMPLOYER USER ADDRESS 1",
		"EMPLOYER USER ADDRESS 2",
		"EMPLOYER USER CITY",
		"EMPLOYER USER STATE",
		"EMPLOYER USER POSTAL CODE",
		"EMPLOYER USER COUNTRY",
		"EMPLOYER USER EMAIL CONFIRMED",
		"PROFILE COMPLETE" as "EMPLOYER USER PROFILE COMPLETE"
	from employeruserds comc join employerwf ewf on cast(comc."EMPLOYER COMPANY ID" as int) = cast(ewf."EMPLOYER ID" as int)
),
jobpostingselectcolumn2 as (
	select 
		CAST(NULL AS INTEGER) as "EMPLOYER USER ID",
		CAST(NULL AS DATE) as "POSTING CREATION DATE",
		CAST(NULL AS INTEGER)  as "POSTING ID",
		CAST(NULL AS VARCHAR) as "POSTING STATUS",
		CAST(NULL AS NUMERIC) as "FEE AMOUNT",
		CAST(NULL AS NUMERIC) as "ENGMT TOTAL",
		CAST(NULL AS VARCHAR) as "INTAKE SESSION STATUS",
		CAST(NULL AS NUMERIC) as "SUBMISSIONS ACTIVE",
		CAST(NULL AS NUMERIC) as "SUBMISSIONS ACTIVE INTERVIEWS",
		CAST(NULL AS INTEGER) as "HIRE NUMBER HIRED",
		CAST(NULL AS INTEGER) as "ENGMT CURRENT PENDING REQUESTS",
		CAST(NULL AS INTEGER) as "PENDING AWARD WHERE CANDIDATE START DATE HAS PASSED(#)",
		CAST(NULL AS DATE) as "INITIAL MARKETPLACE DATE"
),
addformula1 as (
	select *,
		CASE WHEN "POSTING STATUS" = 'Open' THEN 1 ELSE 0 END AS "CURRENT OPEN POSTS COUNT",
		CASE WHEN "POSTING STATUS" = 'Open' THEN "FEE AMOUNT" ELSE 0 END  as "CURRENT OPEN POSTS FEE AMOUNT",
		CASE WHEN "INTAKE SESSION STATUS" = 'OCCURRED' THEN 1 ELSE 0 end as "INTAKE SESSIONS HELD",
		CASE WHEN "ENGMT TOTAL" >= 2 THEN 1 ELSE 0 end as "TOTAL POSTINGS WITH COMPETITION",
		CASE when "POSTING STATUS" = 'ATS Hold' THEN 1 ELSE 0 END as "# OF JOBS IN ATS HOLD",
		CASE WHEN "POSTING STATUS" = 'Open' AND "ENGMT TOTAL" <1 THEN 1 ELSE 0 END as "ACTIVE JOBS WITH 0 ENGMT(#)",
		CASE WHEN "POSTING STATUS" = 'Open' AND "SUBMISSIONS ACTIVE" < 1 THEN 1 ELSE 0 END as "ACTIVE JOBS WITH LESS THAN 1 ACTIVE SUBMISSION(#)",
		CASE WHEN "SUBMISSIONS ACTIVE INTERVIEWS" > 0 THEN 1 ELSE 0 END as "JOBS WITH ACTIVE INTERVIEW(#)",
		CASE 
		  WHEN EXTRACT(MONTH FROM "POSTING CREATION DATE") IN (1, 2, 3)
			AND EXTRACT(YEAR FROM "POSTING CREATION DATE") = EXTRACT(YEAR FROM CURRENT_DATE)
		  THEN 1 
		  ELSE 0 
		END as "Q1 POSTS",
		CASE 
		  WHEN EXTRACT(MONTH FROM "POSTING CREATION DATE") IN (4, 5, 6)
			AND EXTRACT(YEAR FROM "POSTING CREATION DATE") = EXTRACT(YEAR FROM CURRENT_DATE)
		  THEN 1 
		  ELSE 0 
		END as "Q2 POSTS",
		CASE 
		  WHEN EXTRACT(MONTH FROM "POSTING CREATION DATE") IN (7, 8, 9)
			AND EXTRACT(YEAR FROM "POSTING CREATION DATE") = EXTRACT(YEAR FROM CURRENT_DATE)
		  THEN 1 
		  ELSE 0 
		END as "Q3 POSTS",
		CASE 
		  WHEN EXTRACT(MONTH FROM "POSTING CREATION DATE") IN (10, 11, 12)
			AND EXTRACT(YEAR FROM "POSTING CREATION DATE") = EXTRACT(YEAR FROM CURRENT_DATE)
		  THEN 1 
		  ELSE 0 
		END as "Q4 POSTS",
		CASE 
		  WHEN EXTRACT(YEAR FROM "POSTING CREATION DATE") = EXTRACT(YEAR FROM CURRENT_DATE)
		  THEN 1 
		  ELSE 0 
		END AS "QTD POSTS",
		CASE 
		  WHEN "ENGMT CURRENT PENDING REQUESTS" > 0 
		  THEN 1 
		  ELSE 0 
		END AS "# jobs that have Pending Engagement",
		CASE 
		  WHEN "POSTING STATUS" = 'Open' AND "INITIAL MARKETPLACE DATE" IS NOT NULL 
		  THEN 1 
		  ELSE 0 
		END AS "CURRENT OPEN MARKETPLACE POST COUNT"
	from jobpostingselectcolumn2
),
GroupingAddformula1Data as (
 SELECT
   "EMPLOYER USER ID",
	MIN("POSTING CREATION DATE") AS "EMPLOYER USER FIRST POST",
	MAX("POSTING CREATION DATE") AS "EMPLOYER USER MOST RECENT POST",
	SUM("CURRENT OPEN POSTS COUNT"::integer) AS "CURRENT OPEN POSTS (#)",
	SUM("CURRENT OPEN POSTS FEE AMOUNT"::numeric) AS "CURRENT OPEN POSTS ($)",
	COUNT(DISTINCT "POSTING ID") AS "TOTAL POSTS (#)",
    SUM("FEE AMOUNT"::numeric) AS "TOTAL POSTS ($)",
    SUM("INTAKE SESSIONS HELD"::integer) AS "TOTAL INTAKE SESSIONS HELD",
    SUM("TOTAL POSTINGS WITH COMPETITION"::integer) AS "TOTAL POSTINGS WITH COMPETITION",
    SUM("# OF JOBS IN ATS HOLD"::integer) AS "TOTAL JOBS IN ATS HOLD",
    SUM("ACTIVE JOBS WITH 0 ENGMT(#)"::integer) AS "ACTIVE JOBS WITH 0 ENGMT(#)",
    SUM("ACTIVE JOBS WITH LESS THAN 1 ACTIVE SUBMISSION(#)"::integer) AS "ACTIVE JOBS WITH LESS THAN 1 ACTIVE S",
    SUM("JOBS WITH ACTIVE INTERVIEW(#)"::integer) AS "JOBS WITH ACTIVE INTERVIEW(#)",
    SUM("HIRE NUMBER HIRED"::integer) AS "EMPLOYER USER NUMBER HIRED",
    SUM("Q1 POSTS"::integer) AS "Q1 POSTS",
    SUM("Q2 POSTS"::integer) AS "Q2 POSTS",
    SUM("Q3 POSTS"::integer) AS "Q3 POSTS",
    SUM("Q4 POSTS"::integer) AS "Q4 POSTS",
    SUM("QTD POSTS"::integer) AS "QTD POSTS",
    SUM("# jobs that have Pending Engagement"::integer) AS "JOBS WITH A PENDING ENGAGEMENT (#)",
    sum("PENDING AWARD WHERE CANDIDATE START DATE HAS PASSED(#)") as "PENDING AWARD WHERE CANDIDATE START DATE HAS PASSED(#)",
    sum("CURRENT OPEN MARKETPLACE POST COUNT") as "CURRENT OPEN MARKETPLACE POSTS(#)"
   from addformula1
  group by "EMPLOYER USER ID"
),
 joindata2component AS (
  SELECT 
  jsd."EMPLOYER USER ID" as "EMPLOYER USER ID_jsd",
  jsd."EMPLOYER_COMPANY_ID",*
  FROM "joindata1ndselectdata1" jsd 
  LEFT OUTER JOIN "GroupingAddformula1Data" gad  
    ON gad."EMPLOYER USER ID" = jsd."EMPLOYER USER ID"
),
Addformula AS (
  SELECT 
    *,
    COALESCE("TOTAL POSTS (#)", 0) AS "EMPLOYER USER TOTAL POSTS (#)",
    COALESCE("TOTAL POSTS ($)", 0) AS "EMPLOYER USER TOTAL POSTS ($)",
    CASE 
      WHEN "TOTAL POSTS (#)" IS NOT NULL AND "TOTAL POSTS (#)" != 0 
      THEN ("TOTAL INTAKE SESSIONS HELD"::FLOAT / "TOTAL POSTS (#)") * 100 
      ELSE 0 
    END AS "EMPLOYER USER % OF INTAKE SESSIONS HELD",
    CASE 
      WHEN "TOTAL POSTS (#)" IS NOT NULL AND "TOTAL POSTS (#)" != 0 
      THEN ("TOTAL POSTINGS WITH COMPETITION"::FLOAT / "TOTAL POSTS (#)") * 100 
      ELSE 0 
    END AS "EMPLOYER USER % TIME ENGAGING COMPETITION",
    (CURRENT_DATE - "EMPLOYER USER LAST LOGIN") + 30 AS "DAYS TILL DROP",
    CASE
      WHEN "USER_HANDLER_NAME" = 'WorkableHiringManager' THEN 'WORKABLE'
      WHEN "USER_HANDLER_NAME" = 'ConnectHiringManager' THEN 'Connect Direct'
      WHEN "USER_HANDLER_NAME" = 'GreenhouseHiringManager' THEN 'Greenhouse'
      ELSE 'STANDARD'
    END AS "USER SOURCE"
  FROM joindata2component
),finalcolumnselect as (
select 
   "EMPLOYER USER ID_jsd" as "EMPLOYER USER ID",
    "EMPLOYER USER CREATION DATE",
    "EMPLOYER USER STATUS",
    "EMPLOYER USER JOB TITLE",
    "EMPLOYER USER BUSINESS UNIT",
    "EMPLOYER COMPANY ID",
    "EMPLOYER USER EMAIL ADDRESS",
    "EMPLOYER USER PHONE",
    "EMPLOYER USER LAST LOGIN",
    "FULL NAME",
    "EMPLOYER ID",
    "EMPLOYER NAME",
    "EMPLOYER CUSTOMER TYPE",
    "EMPLOYER CREATION DATE",
    "EMPLOYER LAUNCH DATE",
--    "EMPLOYER LAST USER LOGIN",
    "EMPLOYER FIRST POST DATE",
    "EMPLOYER LAST POST DATE",
    "EMPLOYER ATS VENDOR",
    "EMPLOYER POSTING ALLOWED",
    "EMPLOYER USER FIRST NAME",
    "EMPLOYER USER LAST NAME",
    "ACCOUNT DIRECTOR",
    "ACCOUNT DIRECTOR EMAIL",
    "ACCOUNT MANAGER",
    "ACCOUNT MANAGER EMAIL",
    "EMPLOYER USER FIRST POST",
    "EMPLOYER USER MOST RECENT POST",
    "CURRENT OPEN POSTS (#)",
    "CURRENT OPEN POSTS ($)",
    "TOTAL POSTS (#)",
    "TOTAL POSTS ($)",
    "TOTAL INTAKE SESSIONS HELD",
    "EMPLOYER USER % OF INTAKE SESSIONS HELD",
    "TOTAL POSTINGS WITH COMPETITION",
    "EMPLOYER USER % TIME ENGAGING COMPETITION",
    "TOTAL JOBS IN ATS HOLD",
    "ACTIVE JOBS WITH 0 ENGMT(#)",
    "ACTIVE JOBS WITH LESS THAN 1 ACTIVE S",
    "JOBS WITH ACTIVE INTERVIEW(#)",
    "EMPLOYER USER NUMBER HIRED",
    "DAYS TILL DROP",
    "Q1 POSTS",
    "Q2 POSTS",
    "Q3 POSTS",
    "Q4 POSTS",
    "QTD POSTS",
    "USER SOURCE",
    "JOBS WITH A PENDING ENGAGEMENT (#)",
    "EMPLOYER COMPANY SIZE",
    "PENDING AWARD WHERE CANDIDATE START DATE HAS PASSED(#)",
    "CURRENT OPEN MARKETPLACE POSTS(#)",
    "USER TYPE",
    "EMPLOYER USER LAST MODIFIED DATE",
    "EMPLOYER USER ADDRESS 1",
    "EMPLOYER USER ADDRESS 2",
    "EMPLOYER USER CITY",
    "EMPLOYER USER STATE",
    "EMPLOYER USER POSTAL CODE",
    "EMPLOYER USER COUNTRY",
    "EMPLOYER USER EMAIL CONFIRMED",
    "EMPLOYER USER PROFILE COMPLETE"
from Addformula
)
,ZIPS_TO_FIPS_SELECTCOLUMN3 as (
select 
        CAST(NULL AS TEXT) AS "FIPS_STRING",
		CAST(NULL AS TEXT) AS "CITY",
		CAST(NULL AS TEXT) AS "STATE",
		CAST(NULL AS TEXT) AS "CNTY_NAME",
		CAST(NULL AS INTEGER) AS "ZIP_STRING_LEADING_ZERO"
from bounty_jobs.postal_code
)
,JOINDATA1COMPONENT as (
select
fc.*,
"FIPS_STRING" as "EMPLOYER_USER_FIPS_ZIP",
"CITY" as "EMPLOYER_USER_FIPS_CITY",
"STATE" as "EMPLOYER_USER_FIPS_STATE",
"CNTY_NAME" as "EMPLOYER_USER_FIPS_COUNTY",
"ZIP_STRING_LEADING_ZERO" as "EMPLOYER_USER_FIPS_CODE"
from finalcolumnselect fc 
left outer join ZIPS_TO_FIPS_SELECTCOLUMN3 sc on fc."EMPLOYER USER POSTAL CODE" = sc."ZIP_STRING_LEADING_ZERO" 
)
,MEETINGS_TO_COMPANIES as (
select
null as "createdAt",
null as "archived",
null as "properties_hs_createdate",
null as "properties_hs_lastmodifieddate",
null as "id",
CAST(NULL AS INTEGER) as "properties_hs_object_id",
null as "updatedAt",
CAST(NULL AS INTEGER) as "associations_companies_results_id",
CAST(NULL AS TEXT) as "associations_companies_results_type"
)
,hubspot_meetings as (
SELECT
  CAST(NULL AS INTEGER) AS "id",
  CAST(NULL AS TEXT) AS "properties_hs_activity_type",
  CAST(NULL AS INTEGER) AS "properties_hs_all_accessible_team_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_all_assigned_business_unit_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_all_owner_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_all_team_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_at_mentioned_owner_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_attachment_ids",
  CAST(NULL AS INTEGER) AS "properties_hs_attendee_owner_ids",
  CAST(NULL AS TEXT) AS "properties_hs_body_preview",
  CAST(NULL AS TEXT) AS "properties_hs_body_preview_html",
  CAST(NULL AS TEXT) AS "properties_hs_body_preview_is_truncated",
  CAST(NULL AS DATE) AS "properties_hs_contact_first_outreach_date",
  CAST(NULL AS TEXT) AS "properties_hs_created_by",
  CAST(NULL AS INTEGER) AS "properties_hs_created_by_user_id",
  CAST(NULL AS DATE) AS "properties_hs_createdate",
  CAST(NULL AS TEXT) AS "properties_hs_engagement_source",
  CAST(NULL AS INTEGER) AS "properties_hs_engagement_source_id",
  CAST(NULL AS TEXT) AS "properties_hs_follow_up_action",
  CAST(NULL AS TEXT) AS "properties_hs_gdpr_deleted",
  CAST(NULL AS TEXT) AS "properties_hs_guest_emails",
  CAST(NULL AS TEXT) AS "properties_hs_i_cal_uid",
  CAST(NULL AS TEXT) AS "properties_hs_include_description_in_reminder",
  CAST(NULL AS TEXT) AS "properties_hs_internal_meeting_notes",
  CAST(NULL AS DATE) AS "properties_hs_lastmodifieddate",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_body",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_calendar_event_hash",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_change_id",
  CAST(NULL AS INTEGER) AS "properties_hs_meeting_created_from_link_id",
  CAST(NULL AS TIMESTAMP) AS "properties_hs_meeting_end_time",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_external_url",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_location",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_location_type",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_ms_teams_payload",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_outcome",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_payments_session_id",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_pre_meeting_prospect_reminders",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_source",
  CAST(NULL AS INTEGER) AS "properties_hs_meeting_source_id",
  CAST(NULL AS TIMESTAMP) AS "properties_hs_meeting_start_time",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_title",
  CAST(NULL AS TEXT) AS "properties_hs_meeting_web_conference_meeting_id",
  CAST(NULL AS TEXT) AS "properties_hs_merged_object_ids",
  CAST(NULL AS TEXT) AS "properties_hs_modified_by",
  CAST(NULL AS INTEGER) AS "properties_hs_object_id",
  CAST(NULL AS TEXT) AS "properties_hs_object_source",
  CAST(NULL AS TEXT) AS "properties_hs_object_source_detail_1",
  CAST(NULL AS TEXT) AS "properties_hs_object_source_detail_2",
  CAST(NULL AS TEXT) AS "properties_hs_object_source_detail_3",
  CAST(NULL AS INTEGER) AS "properties_hs_object_source_id",
  CAST(NULL AS TEXT) AS "properties_hs_object_source_label",
  CAST(NULL AS INTEGER) AS "properties_hs_object_source_user_id",
  CAST(NULL AS TEXT) AS "properties_hs_outcome_canceled_count",
  CAST(NULL AS TEXT) AS "properties_hs_outcome_completed_count",
  CAST(NULL AS TEXT) AS "properties_hs_outcome_no_show_count",
  CAST(NULL AS TEXT) AS "properties_hs_outcome_rescheduled_count",
  CAST(NULL AS TEXT) AS "properties_hs_outcome_scheduled_count",
  CAST(NULL AS TEXT) AS "properties_hs_product_name",
  CAST(NULL AS TEXT) AS "properties_hs_queue_membership_ids",
  CAST(NULL AS TEXT) AS "properties_hs_read_only",
  CAST(NULL AS TEXT) AS "properties_hs_roster_object_coordinates",
  CAST(NULL AS TEXT) AS "properties_hs_scheduled_tasks",
  CAST(NULL AS TEXT) AS "properties_hs_time_to_book_meeting_from_first_contact",
  CAST(NULL AS TIMESTAMP) AS "properties_hs_timestamp",
  CAST(NULL AS TEXT) AS "properties_hs_timezone",
  CAST(NULL AS TEXT) AS "properties_hs_unique_creation_key",
  CAST(NULL AS INTEGER) AS "properties_hs_unique_id",
  CAST(NULL AS INTEGER) AS "properties_hs_updated_by_user_id",
  CAST(NULL AS INTEGER) AS "properties_hs_user_ids_of_all_notification_followers",
  CAST(NULL AS INTEGER) AS "properties_hs_user_ids_of_all_notification_unfollowers",
  CAST(NULL AS INTEGER) AS "properties_hs_user_ids_of_all_owners",
  CAST(NULL AS TEXT) AS "properties_hs_was_imported",
  CAST(NULL AS DATE) AS "properties_hubspot_owner_assigneddate",
  CAST(NULL AS INTEGER) AS "properties_hubspot_owner_id",
  CAST(NULL AS INTEGER) AS "properties_hubspot_team_id",
  CAST(NULL AS TIMESTAMP) AS "createdAt",
  CAST(NULL AS TIMESTAMP) AS "updatedAt",
  CAST(NULL AS TEXT) AS "archived",
  CAST(NULL AS TIMESTAMP) AS "persistenceTimestamp",
  CAST(NULL AS TEXT) AS "sensitivityLevel",
  CAST(NULL AS TEXT) AS "isDeleted",
  CAST(NULL AS TEXT) AS "isEncrypted",
  CAST(NULL AS TEXT) AS "sourceUpstreamDeployable",
  CAST(NULL AS TEXT) AS "requestId"
)
,JOINDATA3COMPONENT as (
select
mc."associations_companies_results_id",
mc."associations_companies_results_type",
hm."id",
hm."properties_hs_activity_type",
hm."properties_hs_all_accessible_team_ids",
hm."properties_hs_all_assigned_business_unit_ids",
hm."properties_hs_all_owner_ids",
hm."properties_hs_all_team_ids",
hm."properties_hs_at_mentioned_owner_ids",
hm."properties_hs_attachment_ids",
hm."properties_hs_attendee_owner_ids",
hm."properties_hs_body_preview",
hm."properties_hs_body_preview_html",
hm."properties_hs_body_preview_is_truncated",
hm."properties_hs_contact_first_outreach_date",
hm."properties_hs_created_by",
hm."properties_hs_created_by_user_id",
hm."properties_hs_createdate",
hm."properties_hs_engagement_source",
hm."properties_hs_engagement_source_id",
hm."properties_hs_follow_up_action",
hm."properties_hs_gdpr_deleted",
hm."properties_hs_guest_emails",
hm."properties_hs_i_cal_uid",
hm."properties_hs_include_description_in_reminder",
hm."properties_hs_internal_meeting_notes",
hm."properties_hs_lastmodifieddate",
hm."properties_hs_meeting_body",
hm."properties_hs_meeting_calendar_event_hash",
hm."properties_hs_meeting_change_id",
hm."properties_hs_meeting_created_from_link_id",
hm."properties_hs_meeting_end_time",
hm."properties_hs_meeting_external_url",
hm."properties_hs_meeting_location",
hm."properties_hs_meeting_location_type",
hm."properties_hs_meeting_ms_teams_payload",
hm."properties_hs_meeting_outcome",
hm."properties_hs_meeting_payments_session_id",
hm."properties_hs_meeting_pre_meeting_prospect_reminders",
hm."properties_hs_meeting_source",
hm."properties_hs_meeting_source_id",
hm."properties_hs_meeting_start_time",
hm."properties_hs_meeting_title",
hm."properties_hs_meeting_web_conference_meeting_id",
hm."properties_hs_merged_object_ids",
hm."properties_hs_modified_by",
hm."properties_hs_object_id",
hm."properties_hs_object_source",
hm."properties_hs_object_source_detail_1",
hm."properties_hs_object_source_detail_2",
hm."properties_hs_object_source_detail_3",
hm."properties_hs_object_source_id",
hm."properties_hs_object_source_label",
hm."properties_hs_object_source_user_id",
hm."properties_hs_outcome_canceled_count",
hm."properties_hs_outcome_completed_count",
hm."properties_hs_outcome_no_show_count",
hm."properties_hs_outcome_rescheduled_count",
hm."properties_hs_outcome_scheduled_count",
hm."properties_hs_product_name",
hm."properties_hs_queue_membership_ids",
hm."properties_hs_read_only",
hm."properties_hs_roster_object_coordinates",
hm."properties_hs_scheduled_tasks",
hm."properties_hs_time_to_book_meeting_from_first_contact",
hm."properties_hs_timestamp",
hm."properties_hs_timezone",
hm."properties_hs_unique_creation_key",
hm."properties_hs_unique_id",
hm."properties_hs_updated_by_user_id",
hm."properties_hs_user_ids_of_all_notification_followers",
hm."properties_hs_user_ids_of_all_notification_unfollowers",
hm."properties_hs_user_ids_of_all_owners",
hm."properties_hs_was_imported",
hm."properties_hubspot_owner_assigneddate",
hm."properties_hubspot_owner_id",
hm."properties_hubspot_team_id",
hm."createdAt",
hm."updatedAt",
hm."archived",
hm."persistenceTimestamp",
hm."sensitivityLevel",
hm."isDeleted",
hm."isEncrypted",
hm."sourceUpstreamDeployable",
hm."requestId"
from MEETINGS_TO_COMPANIES mc 
inner join hubspot_meetings hm on  mc."properties_hs_object_id" = hm."id"
)
, QBR_COMPONENT as (
select
*
from JOINDATA3COMPONENT jdc
where
jdc."properties_hs_activity_type" like '%QBR%'
)
,Company_id_component as (
select 
"associations_companies_results_id",
MAX("properties_hs_meeting_start_time") as EMPLOYER_COMPANY_LAST_QBR
from QBR_COMPONENT
group by "associations_companies_results_id"
)
,Custom_Objects_Companies as (
select
CAST(NULL AS INTEGER) AS "hubportalid",
CAST(NULL AS INTEGER) AS "hubcompanyid",
CAST(NULL AS TEXT) AS "hubcompanylifecycle",
CAST(NULL AS TEXT) AS "hubcompanyname",
CAST(NULL AS INTEGER) AS "hubcompanyownerid",
CAST(NULL AS TEXT) AS "hubcompanytype",
CAST(NULL AS TEXT) AS "hubcompanyownername",
CAST(NULL AS TEXT) AS "hubcompanyowneremail",
CAST(NULL AS INTEGER) AS "hubbojoobjectid",
CAST(NULL AS INTEGER) AS "hubbojositeid",
CAST(NULL AS TEXT) AS "hubbojoname",
CAST(NULL AS INTEGER) AS "hubbojonameid",
CAST(NULL AS INTEGER) AS "rn"
)
,joinData4component as (
select *
from Company_id_component cc 
right outer join Custom_Objects_Companies co on cc.associations_companies_results_id = co.hubcompanyid 
)
,hubspot_companies as (
select 
properties__type__value as "HubSpot_Company_Type",
companyid,
properties__lifecyclestage__value as "HubSpot_Lifecycle_Stage"
from hubspot.companies hc
)
,AddFormula2 as (
select
*,
(CASE 
 	WHEN "HubSpot_Lifecycle_Stage" IN ('34428698') THEN 'Former Customer' 
 	ELSE "HubSpot_Lifecycle_Stage" 
 END) as "HubSpot_Lifecycle_Stage"
from hubspot_companies 
)
,JoinData6_Component as (
select *
from joinData4component dc
inner join AddFormula2 af on dc.hubcompanyid = af.companyid 
),prefinal as (
select j1d.*
from JOINDATA1COMPONENT j1d 
left outer join JoinData6_Component j6d on j1d."EMPLOYER COMPANY ID" = j6d."hubbojositeid"
)
select * from JOINDATA1COMPONENT