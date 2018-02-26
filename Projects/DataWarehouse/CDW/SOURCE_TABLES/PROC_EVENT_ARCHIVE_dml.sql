alter session enable parallel dml;

set timi on

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string 
from event e
join proc_event_archive_cbn r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_gp1 r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_gp2 r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_nbn r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_nbx r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_qhn r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_sbn r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

insert --+ parallel(32)
into proc_event_archive
select r.network, r.visit_id, r.event_id, r.archive_number, r.archive_type_id, r.emp_provider_id, r.event_status_id, r.archive_time, r.arch_comment, r.result_report_nbr, r.review_comment, r.order_visit_id, r.spec_coll_time, r.spec_coll_emp_provider_id, r.off_line_doc_time, r.off_line_emp_provider_id, r.spec_receiving_area_id, r.spec_auto_accept_flg, r.cid, r.device_id, r.scheduled_abs_time_type_id, r.scheduled_abs_time, r.context_visit_id, r.scheduled_abs_end_time, r.scheduled_abs_string
from event e
join proc_event_archive_smn r on r.event_id = e.event_id and r.visit_id = e.visit_id and r.network = e.network;

commit;

alter session force parallel dml parallel 32;

CREATE UNIQUE INDEX pk_proc_event_archive ON proc_event_archive(event_id, visit_id, archive_number, network) LOCAL PARALLEL 32;
ALTER INDEX pk_proc_event_archive NOPARALLEL;

CREATE INDEX idx_proc_event_archive_cid ON proc_event_archive(cid, network) LOCAL PARALLEL 32;
ALTER INDEX idx_proc_event_archive_cid NOPARALLEL;

ALTER TABLE proc_event_archive ADD CONSTRAINT pk_proc_event_archive
 PRIMARY KEY(network, visit_id, event_id, archive_number) USING INDEX pk_proc_event_archive;
