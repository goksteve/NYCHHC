  SELECT
   DISTINCT x.network,
            x.facility_name,
            x.facility_id,
            pharm_subclass_c,
            w.name pharmacy_class,
            a.pat_id,
            e.pat_mrn_id,
            INITCAP(e.pat_name) pat_name,
           a.PAT_ENC_CSN_ID as order_visit_id,
            e.add_line_1,
            e.add_line_2,
            e.city,
            e.zip,
            e.sex_c,
            z4.name gender,
            e.birth_date,
            ROUND(MONTHS_BETWEEN(SYSDATE, e.birth_date) / 12, 1) AS age_at_rx,
            '-99' rx_allow_subs,
            -99 rx_pool_id,
            -99 rx_archive_nbr,
            -99 rx_number,
            -99 prescription_event_type_id,
            -99 prescription_arch_action_id,
            -99 prescription_type_id,
            z5.name race,
            -99 eid,
            --CONVERT(int,ROUND(DATEDIFF(hour,E.BIRTH_DATE,GETDATE())/8766.0,0)) AS AGE,
            a.ordering_date,
            a.pat_enc_date_real,
            TO_NUMBER(a.pat_enc_csn_id, 999999999999) AS pat_enc_csn_id,
            --R.DX_ID, S.DX_NAME,
            TO_NUMBER(a.medication_id, 999999999999) AS medication_id,
            g.name medication_name,
            a.display_name,
            g.form,
            g.route medication_indicated_route,
            a.med_route_c,
            p.name adminstered_route,
            a.min_discrete_dose,
            a.max_discrete_dose,
            TO_CHAR(a.dose_unit_c) AS dose_unit_c,
            a.act_order_c,
            q.name med_order_status,
            a.med_comments AS rx_comment,
            a.order_status_c,
            o.name prescription_status_name,
            TO_NUMBER(a.order_med_id) AS order_med_id,
            a.order_class_c,
            h.name ordering_class_name,
            a.pharmacy_id,
            i.pharmacy_name,
            i.formulary_id,
            i.pres_formulary_id,
            i.disp_int_pp_id,
            a.sig,
            TO_NUMBER(REGEXP_REPLACE(a.quantity, '[^0-9]', ''), 99999) AS quantity,
            TO_NUMBER(a.refills, 999) AS refills,
            a.pend_action_c,
            n.name pend_acttion_reorder_name,
            a.disp_as_written_yn,
            a.med_presc_prov_id,
            a.nonfrm_xcpt_cd_c,
            m.name nonfrm_xcpt_name,
            a.pat_loc_id,
            c.department_id,
            INITCAP(loc_name) loc_name,
            a.update_date,
            a.order_inst,
            a.order_priority_c,
            j.name ordering_priority_name,
            a.chng_order_med_id,
            a.start_date,
            a.end_date,
            a.order_start_time,
            a.order_end_time,
            CAST(z.prescrip_exp_date AS DATE) AS prescrip_exp_date,
            a.discon_time,
            a.non_formulary_yn,
            TO_NUMBER(NVL(a.ord_prov_id, 999999999999), 999999999999) AS ord_prov_id,
            INITCAP(z2.prov_name) ord_prov_name,
            TO_NUMBER(NVL(a.authrzing_prov_id, 999999999999), 999999999999) AS authrzing_prov_id,
            INITCAP(z3.prov_name) auth_prov_name,
            a.provider_type_c,
            z1.name provider_type,
            a.is_pending_ord_yn,
            a.sched_start_tm,
            a.med_comments,
            a.mdl_id,
            a.lastdose,
            a.refills_remaining,
            a.med_refill_prov_id,
            a.rule_based_ord_t_yn,
            a.ordering_mode_c,
            l.name ordering_mode_name,
            a.pend_approve_flag_c,
            a.prov_status_c,
            a.nf_post_verif_yn,
            a.max_dose,
            a.max_dose_unit_c,
            a.prn_comment,
            a.med_dis_disp_qty,
            a.med_dis_disp_unit_c,
            a.end_before_cmp_inst,
            a.last_dose_time,
            a.hv_is_self_adm_yn,
            a.hv_hospitalist_yn,
            a.hv_discr_freq_id,
            a.hv_discrete_dose,
            a.hv_dose_unit_c,
            u.med_linked_proc_id,
            v.proc_name,
            TO_NUMBER(NVL(a.order_status_c, 9999), 9999) AS order_status_code,
            x.name order_status_description,
            a.rsn_for_discon_c,
            k.name discon_reason,
            z.rec_archived_yn
  FROM
      epic_clarity.order_med a
   LEFT OUTER JOIN epic_clarity.patient_3 b ON b.pat_id = a.pat_id
   LEFT OUTER JOIN epic_clarity.clarity_dep c ON c.department_id = a.pat_loc_id
   LEFT OUTER JOIN clarity_loc d ON d.loc_id = c.rev_loc_id
   LEFT OUTER JOIN epic_clarity.x_loc_facility_mapping x ON x.facility_id = d.adt_parent_id
   LEFT OUTER JOIN epic_clarity.patient e ON e.pat_id = a.pat_id
   LEFT OUTER JOIN epic_clarity.clarity_medication g ON a.medication_id = g.medication_id
   LEFT OUTER JOIN epic_clarity.zc_order_class h ON h.order_class_c = a.order_class_c
   LEFT OUTER JOIN epic_clarity.rx_phr i ON i.pharmacy_id = a.pharmacy_id
   LEFT OUTER JOIN epic_clarity.zc_order_priority j ON j.order_priority_c = a.order_priority_c
   LEFT OUTER JOIN epic_clarity.zc_rsn_for_discon k ON k.rsn_for_discon_c = a.rsn_for_discon_c
   LEFT OUTER JOIN epic_clarity.zc_ordering_mode l ON l.ordering_mode_c = a.ordering_mode_c
   LEFT OUTER JOIN epic_clarity.zc_nonfrm_xcpt_cd m ON m.nonfrm_xcpt_cd_c = a.nonfrm_xcpt_cd_c
   LEFT OUTER JOIN epic_clarity.zc_pend_action n ON n.pend_action_c = a.pend_action_c
   LEFT OUTER JOIN epic_clarity.zc_order_status o ON o.order_status_c = a.order_status_c
   LEFT OUTER JOIN epic_clarity.zc_admin_route p ON p.med_route_c = a.med_route_c
   LEFT OUTER JOIN epic_clarity.zc_active_order q ON q.active_order_c = a.act_order_c
   LEFT OUTER JOIN epic_clarity.order_dx_med r ON r.order_med_id = a.order_med_id
   LEFT OUTER JOIN epic_clarity.clarity_edg s ON s.dx_id = r.dx_id
   LEFT OUTER JOIN epic_clarity.order_medinfo u ON u.order_med_id = a.order_med_id
   LEFT OUTER JOIN epic_clarity.clarity_eap v ON v.proc_id = u.med_linked_proc_id
   LEFT OUTER JOIN epic_clarity.zc_pharm_class w ON w.pharm_class_c = g.pharm_class_c
   LEFT OUTER JOIN epic_clarity.zc_order_status x ON x.order_status_c = a.order_status_c
   LEFT OUTER JOIN epic_clarity.order_med_3 z ON z.order_id = a.order_med_id
   LEFT OUTER JOIN epic_clarity.zc_provider_type z1 ON z1.provider_type_c = a.provider_type_c
   LEFT OUTER JOIN epic_clarity.clarity_ser z2 ON z2.prov_id = a.ord_prov_id
   LEFT OUTER JOIN epic_clarity.clarity_ser z3 ON z3.prov_id = a.authrzing_prov_id
   LEFT OUTER JOIN epic_clarity.zc_pref_pcp_sex z4 ON z4.pref_pcp_sex_c = e.sex_c
   LEFT OUTER JOIN epic_clarity.zc_ethnic_group z5 ON e.ethnic_group_c = z5.ethnic_group_c
  WHERE
   (b.is_test_pat_yn <> 'Y')
   AND a.ordering_mode_c != 2 -- REMOVING INPATIENTS
   AND CAST(a.ordering_date AS DATE) BETWEEN '01-APR-2016' AND TRUNC(SYSDATE, 'MM') -- FOR APPEND DATA NEXT MONTH
   AND a.pend_action_c IN (2,
                           6,
                           1,
                           5,
                           7) --Reorder,Reorder from Order Review,Change,Reorder from Medication Activity,Reorder from Reports