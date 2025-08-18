-- Data Source : BountyJobs | SQL | Hire : https://bountyjobs.domo.com/datasources/17029e35-bbb0-450f-93e5-7263f0dc302c/details/settings
{{ config(
    materialized='table',
    tags=['nightly', 'slow'],
    sort='hire_id',
    dist='hire_id'
) }}
select distinct h.hire_id,
    jp.company_id,
    jp.job_posting_id,
    jp.posting_number as bountyjobs_id,
    jp.title,
    jp.workflow_handler_name,
    jp.retained_flag,
	hm.first_name || ' ' || hm.last_name as hiring_manager_name,
	lower(hm.email_address) as hiring_manager_email,
    h.submission_id,
    h.created_by,
    h.creation_date,
    h.modified_by,
    h.modified_date,
    h.comp_owner_id as compensation_owner_id,
    p.payment_status as adjusted_payment_status,
    addr.country as award_address_country,
    addr.state as award_address_state,
    addr.city as award_address_city,
    hmc.payment_term as company_payment_term,
    dateadd(day, hmc.payment_term, h.hire_date) as on_time_payment_date,
	case when p.payment_status = 'P' then
    case when p.count_as_on_time = 'Y' or h.hire_date is null then 'Paid - On Time'
    when p.last_payment_date > dateadd(day, hmc.payment_term, h.hire_date) then 'Paid - Late'
    else 'Paid - On Time' end
	when p.payment_status = 'R' then 'Refunded'
	when p.payment_status = 'U' then 'Unpaid'
	else null end as unadjusted_payment_status,
    h.currency_code,
	case when jp.currency_code = 'USD' then 
      (case when h.hire_date > current_date then jp.creation_date else h.hire_date end)
      else
      (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then coalesce(jp.posting_effective_date, jp.creation_date) else (select cr.effective_date from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then coalesce(jp.posting_effective_date, jp.creation_date) else (select cr.effective_date from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end as currency_conversion_date,
	  round((case when jp.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end),2) as currency_conversion_rate,
	  (select sum(case when a.account_type_code = 'D' then ae.amount else -ae.amount end)
     from {{ source('bounty_jobs', 'transaction') }} t
              join {{ source('bounty_jobs', 'account_entry') }} ae
                   on ae.transaction_id = t.id
              join {{ source('bounty_jobs', 'account') }} a
                   on a.id = ae.account_id
                       and a.account_chart_type_id in (1, 2, 4)
              join {{ source('bounty_jobs', 'company') }} c
                   on c.company_id = a.company_id
                       and c.company_type_code = 'E'
     where t.submission_id = h.submission_id
       and t.transaction_type_id in (6, 12, 24, 25, 26, 27, 29, 30, 31, 32, 33, 36, 40)) as fee_amount,
       (round(((select sum(case when a.account_type_code = 'D' then ae.amount else -ae.amount end)
             from {{ source('bounty_jobs', 'transaction') }} t
                      join {{ source('bounty_jobs', 'account_entry') }} ae
                           on ae.transaction_id = t.id
                      join {{ source('bounty_jobs', 'account') }} a
                           on a.id = ae.account_id
                               and a.account_chart_type_id in (1, 2, 4)
                      join {{ source('bounty_jobs', 'company') }} c
                           on c.company_id = a.company_id
                               and c.company_type_code = 'E'
             where t.submission_id = h.submission_id
               and t.transaction_type_id in (6, 12, 24, 25, 26, 27, 29, 30, 31, 32, 33, 36, 40)) * (case when h.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end)), 2)) as fee_amount_usd_converted,
	h.salary_percent_award,
    h.salary,
	((round((h.salary * (
    case when h.currency_code = 'USD' then 1 else
    (case when h.hire_date > current_date then 
    (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
            where cr.source_currency_code = upper(trim(h.currency_code))
              and cr.target_currency_code = upper(trim('USD'))
              and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
            order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end
          )
	  end)), 2))) as salary_usd_converted,
	h.hire_date as candidate_start_date,
    h.purchase_order,
    h.award,
	(round((h.award * (case when h.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end)), 2)) as award_usd_converted,
	p.paid_outside,
    ps.description as payment_status,
    p.invoice_number,
    astat.description as award_status,
    p.creation_date as payment_date,
    p.created_by as payment_creator_id,
    p.modified_by as payment_modifier_id,
    p.modified_date as payment_modification_date,
    p.last_payment_date,
    case when p.count_as_on_time = 'Y' then true else false end as count_as_on_time,
    hmsg.receiving_person_id as award_acceptor,
    hmsg.creation_date as award_accepted_date,
	case
        when hire_transaction.transaction_type_id = 6 and (datediff(day,orig_rec.initial_activation_date,s.creation_date) < 15 and
                                                           orig_rec.initial_activation_date < '2008-02-01 00:00:00' and
                                                           s.creation_date > '2007-02-01 00:00:00')
            then '100% Quick Draw'
        when hire_transaction.transaction_type_id = 6 and (datediff(day,orig_rec.initial_activation_date,s.creation_date) < 15 and
                                                           orig_rec.initial_activation_date > '2008-02-01 00:00:00' and
                                                           orig_rec.initial_activation_date < '2010-07-24 00:00:00')
            then '97% Quick Draw'
        when hire_transaction.transaction_type_id = 6 and ct.contract_id is not null and
             datediff(day,orig_rec.initial_activation_date,s.creation_date) > 14 and ct.start_date < s.creation_date
            then 'Special Contract'
        when hire_transaction.transaction_type_id = 24 then '97% Quick Draw'
        when hire_transaction.transaction_type_id = 25 then 'Special Contract'
        when hire_transaction.transaction_type_id = 26 then 'Employer Gets Discount'
        when hire_transaction.transaction_type_id = 27 then 'Multi-fill Deal'
        when hire_transaction.transaction_type_id = 29 then 'Payout Override'
        when hire_transaction.transaction_type_id = 36 then 'Retained'
        when hire_transaction.transaction_type_id is null then 'Not Processed Yet'
        when hire_transaction.transaction_type_id = 40 then 'Split'
        else
            case when hire_transaction.transaction_type_id = 6 then 'Standard Terms'
                 else 'Unknown'
                end
        end as type_of_placement,
	(select sum(case when a_in.account_type_code = 'D' then -ae_in.amount else ae_in.amount end)
        from {{ source('bounty_jobs', 'account_entry') }} ae_in
                 join {{ source('bounty_jobs', 'account') }} a_in on a_in.id = ae_in.account_id
        where a_in.account_chart_type_id = 10
          and ae_in.transaction_id = hire_transaction.id
    ) as bountyjobs_net,
	(round((select sum(case when a_in.account_type_code = 'D' then -ae_in.amount else ae_in.amount end)
            from {{ source('bounty_jobs', 'account_entry') }} ae_in
                     join {{ source('bounty_jobs', 'account') }} a_in on a_in.id = ae_in.account_id
            where a_in.account_chart_type_id = 10
              and ae_in.transaction_id = hire_transaction.id) * (case when h.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end), 2)) as bountyjobs_net_usd_converted,
	(select sum(case when a_in.account_type_code = 'D' then -ae_in.amount  else ae_in.amount end)
        from {{ source('bounty_jobs', 'account_entry') }} ae_in
                 join {{ source('bounty_jobs', 'account') }} a_in on ae_in.account_id = a_in.id
                 join {{ source('bounty_jobs', 'account_chart_type') }} act_in on a_in.account_chart_type_id = act_in.id and act_in.display_flag = 'Y'
                 join {{ source('bounty_jobs', 'company') }} c_in on c_in.company_id = a_in.company_id and c_in.company_type_code = 'R'
        where ae_in.transaction_id = hire_transaction.id
    ) as recruiter_net,
	(round((select sum(case when a_in.account_type_code = 'D' then -ae_in.amount  else ae_in.amount end)
            from {{ source('bounty_jobs', 'account_entry') }} ae_in
                     join {{ source('bounty_jobs', 'account') }} a_in on ae_in.account_id = a_in.id
                     join {{ source('bounty_jobs', 'account_chart_type') }} act_in on a_in.account_chart_type_id = act_in.id and act_in.display_flag = 'Y'
                     join {{ source('bounty_jobs', 'company') }} c_in on c_in.company_id = a_in.company_id and c_in.company_type_code = 'R'
            where ae_in.transaction_id = hire_transaction.id) * (case when h.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end), 2)) as recruiter_net_usd_converted,
	  (select sum(case when a_in.account_type_code = 'D' then -ae_in.amount  else ae_in.amount end)
        from {{ source('bounty_jobs', 'account_entry') }} ae_in
                 join {{ source('bounty_jobs', 'account') }} a_in on ae_in.account_id = a_in.id
                 join {{ source('bounty_jobs', 'account_chart_type') }} act_in on a_in.account_chart_type_id = act_in.id and act_in.display_flag = 'Y'
                 join {{ source('bounty_jobs', 'company') }} c_in on c_in.company_id = a_in.company_id and c_in.company_type_code = 'E'
        where ae_in.transaction_id = hire_transaction.id
    ) as employer_net,
	(round((select sum(case when a_in.account_type_code = 'D' then -ae_in.amount  else ae_in.amount end)
            from {{ source('bounty_jobs', 'account_entry') }} ae_in
                     join {{ source('bounty_jobs', 'account') }} a_in on ae_in.account_id = a_in.id
                     join {{ source('bounty_jobs', 'account_chart_type') }} act_in on a_in.account_chart_type_id = act_in.id and act_in.display_flag = 'Y'
                     join {{ source('bounty_jobs', 'company') }} c_in on c_in.company_id = a_in.company_id and c_in.company_type_code = 'E'
            where ae_in.transaction_id = hire_transaction.id) * (case when h.currency_code = 'USD' then 1
    else (case when h.hire_date > current_date then 
      (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= coalesce(jp.posting_effective_date, jp.creation_date)
              order by cr.effective_date desc limit 1) end)
	  else
	  (case when h.currency_code = 'USD' then 1 else (select cr.value from {{ source('bounty_jobs', 'conversion_rate') }} cr
              where cr.source_currency_code = upper(trim(h.currency_code))
                and cr.target_currency_code = upper(trim('USD'))
                and cr.effective_date <= greatest(date_trunc('month', h.creation_date)::date,h.hire_date)
              order by cr.effective_date desc limit 1) end)
	  end ) end), 2)) as employer_net_usd_converted,
	case when hmc.payment_term <= 60 then dateadd(day, 60, h.hire_date)
        else dateadd(day, hmc.payment_term, h.hire_date)
        end as payment_due_date,
	case when vc.contract_id is not null then true else false end as vendor_type,
  	hmes.offer_accepted_date,
	p.invoice_added_date as "Invoice Added Date",
	case when (select sum(tt.revmod) from {{ source('bounty_jobs', 'transaction') }} tt where tt.submission_id = h.submission_id)>0 then 'TRUE' else '' end as "REVMOD"
from {{ source('bounty_jobs', 'hire') }} h
         join {{ source('bounty_jobs', 'submission') }} s on h.submission_id = s.submission_id
         join {{ source('bounty_jobs', 'candidate') }} c on c.candidate_id = s.candidate_id
         join {{ source('bounty_jobs', 'recruiter') }} rec on rec.person_id = c.recruiter_person_id
         join {{ source('bounty_jobs', 'person') }} orig_rec on orig_rec.person_id = rec.person_id
         join {{ source('bounty_jobs', 'job_posting') }} jp on jp.job_posting_id = s.job_posting_id
         join {{ source('bounty_jobs', 'person') }} hm on hm.person_id = jp.hiring_manager_person_id
         join {{ source('bounty_jobs', 'company') }} hmc on hmc.company_id = hm.company_id -- added this for bj-8570
         join {{ source('bounty_jobs', 'company') }} emp_comp on emp_comp.company_id = hm.company_id
         join {{ source('bounty_jobs', 'company') }} rec_comp on rec_comp.company_id = orig_rec.company_id
         join {{ source('bounty_jobs', 'payment') }} p on p.hire_id = h.hire_id
         left join {{ source('bounty_jobs', 'contract') }} ct on emp_comp.company_id = ct.source_party_id
            and (orig_rec.company_id = ct.target_party_id or orig_rec.person_id = ct.target_party_id)
            and s.creation_date >= ct.start_date
            and s.creation_date <= ct.end_date
		 left join {{ source('bounty_jobs', 'contract') }} vc on hmc.company_id   = vc.source_party_id
		 	and vc.target_party_id in (rec.person_id, rec_comp.company_id)
   			and h.hire_date  between vc.start_date and vc.end_date
         left join {{ source('bounty_jobs', 'payment_status') }} ps on ps.code = p.payment_status
         left join {{ source('bounty_jobs', 'award_status') }} astat on astat.code = p.award_status
         join {{ source('bounty_jobs', 'hire_message') }} hmes on hmes.submission_id = h.submission_id
         left join {{ source('bounty_jobs', 'address') }} addr on hmes.worksite_address_id = addr.address_id
         join {{ source('bounty_jobs', 'message') }} hmsg on hmsg.parent_message_id = hmes.message_id
         join {{ source('bounty_jobs', 'hire_response') }} hrmsg on hrmsg.message_id = hmsg.message_id and hrmsg.hire_response_type_code = 'A'
         join (				
             select
					t.id,
					t.transaction_type_id,
					t.submission_id as submission_id
				from {{ source('bounty_jobs', 'transaction') }} t 
				join (select max(t.id) as id, submission_id 
                        from {{ source('bounty_jobs', 'transaction') }} t 
                        where t.transaction_type_id in (6, 24, 25, 26, 27, 29, 36, 40) 
                        group by t.submission_id) subt on subt.id = t.id 
                            and subt.submission_id = t.submission_id
                ) hire_transaction on hire_transaction.submission_id = h.submission_id
				
