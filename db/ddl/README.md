# db/ddl 스키마 정의서

이 문서는 `db/ddl/mimic_ddl.ddl`을 기준으로 현재 스키마 구조를 한국어로 재정리한 문서입니다.

- 기준 파일: `db/ddl/mimic_ddl.ddl`
- 기준 스키마: `sso`
- 문서 범위: 테이블, 인덱스, PK/FK 제약조건
- 제외 항목: `STORAGE`, `PCTFREE`, `TABLESPACE` 등 물리 저장 파라미터 상세 설명

## 1) 객체 요약

| 항목 | 개수 | 비고 |
| --- | ---: | --- |
| 테이블 (`CREATE TABLE`) | 24 | 모두 `sso` 스키마 |
| 인덱스 (`CREATE INDEX`) | 25 | `UNIQUE` 12, `NONUNIQUE` 13 |
| 기본키 (`PRIMARY KEY`) | 12 | 모두 명시적 인덱스 사용 (`USING INDEX`) |
| 외래키 (`FOREIGN KEY`) | 13 | `NOVALIDATE` 1건 (`fk_emar_subject`) |
| 뷰/트리거/프로시저 | 0 | 해당 DDL 기준 |

## 2) 스키마 기준 테이블 정의 (`sso`)

| 테이블 | 컬럼 수 | 컬럼 목록(정의 순서) |
| --- | ---: | --- |
| `admissions` | 16 | subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag |
| `caregiver` | 1 | caregiver_id |
| `chartevents` | 11 | subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime, itemid, value, valuenum, valueuom, warning |
| `d_icd_diagnoses` | 3 | icd_code, icd_version, long_title |
| `d_icd_procedures` | 3 | icd_code, icd_version, long_title |
| `d_items` | 9 | itemid, label, abbreviation, linksto, category, unitname, param_type, lownormalvalue, highnormalvalue |
| `d_labitems` | 4 | itemid, label, fluid, category |
| `datetimeevents` | 10 | subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime, itemid, value, valueuom, warning |
| `diagnoses_icd` | 5 | subject_id, hadm_id, seq_num, icd_code, icd_version |
| `emar` | 12 | subject_id, hadm_id, emar_id, emar_seq, poe_id, pharmacy_id, enter_provider_id, charttime, event_txt, scheduletime, storetime, medication |
| `emar_detail` | 33 | subject_id, emar_id, emar_seq, parent_field_ordinal, administration_type, pharmacy_id, barcode_type, complete_dose_not_given, dose_due, dose_due_unit, dose_given, dose_given_unit, will_remainder_of_dose_be_given, product_amount_given, product_unit, product_code, product_description, product_description_other, prior_infusion_rate, infusion_rate, infusion_rate_adjustment, infusion_rate_adjustment_amount, infusion_rate_unit, route, infusion_complete, completion_interval, new_iv_bag_hung, continued_infusion_in_other_location, side, site, non_formulary_visual_verification, reason_for_no_barcode, restart_interval |
| `icustays` | 8 | subject_id, hadm_id, stay_id, first_careunit, last_careunit, intime, outtime, los |
| `ingredientevents` | 17 | subject_id, hadm_id, stay_id, caregiver_id, starttime, endtime, storetime, itemid, amount, amountuom, rate, rateuom, orderid, linkorderid, statusdescription, originalamount, originalrate |
| `inputevents` | 15 | subject_id, hadm_id, stay_id, starttime, endtime, itemid, amount, amountuom, rate, rateuom, orderid, linkorderid, stopped, originalamount, originalrate |
| `labevents` | 16 | labevent_id, subject_id, hadm_id, specimen_id, itemid, order_provider_id, charttime, storetime, value, valuenum, valueuom, ref_range_lower, ref_range_upper, flag, priority, comments |
| `microbiologyevents` | 25 | microevent_id, subject_id, hadm_id, micro_specimen_id, order_provider_id, chartdate, charttime, spec_itemid, spec_type_desc, test_seq, storedate, storetime, test_itemid, test_name, org_itemid, org_name, isolate_num, quantity, ab_itemid, ab_name, dilution_text, dilution_comparison, dilution_value, interpretation, comments |
| `outputevents` | 9 | subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime, itemid, value, valueuom |
| `patients` | 6 | subject_id, gender, anchor_age, anchor_year, anchor_year_group, dod |
| `poe` | 12 | poe_id, poe_seq, subject_id, hadm_id, ordertime, order_type, order_subtype, transaction_type, discontinue_of_poe_id, discontinued_by_poe_id, order_provider_id, order_status |
| `prescriptions` | 21 | subject_id, hadm_id, pharmacy_id, poe_id, poe_seq, order_provider_id, starttime, stoptime, drug_type, drug, formulary_drug_cd, gsn, ndc, prod_strength, form_rx, dose_val_rx, dose_unit_rx, form_val_disp, form_unit_disp, doses_per_24_hrs, route |
| `procedureevents` | 22 | subject_id, hadm_id, stay_id, caregiver_id, starttime, endtime, storetime, itemid, value, valueuom, location, locationcategory, orderid, linkorderid, ordercategoryname, ordercategorydescription, patientweight, isopenbag, continueinnextdept, statusdescription, originalamount, originalrate |
| `procedures_icd` | 6 | subject_id, hadm_id, seq_num, chartdate, icd_code, icd_version |
| `services` | 5 | subject_id, hadm_id, transfertime, prev_service, curr_service |
| `transfers` | 7 | subject_id, hadm_id, transfer_id, eventtype, careunit, intime, outtime |

## 3) 현재 인덱스 정리 (DDL 기준)

현재 정의된 인덱스는 PK 지원 인덱스와 조회 성능용 보조 인덱스로 구성됩니다.

| 인덱스명 | 대상 테이블 | 컬럼 | 유형 |
| --- | --- | --- | --- |
| `idx_adm_los_expr` | `admissions` | CAST(DISCHTIME AS DATE) - CAST(ADMITTIME AS DATE) | `NONUNIQUE` |
| `idx_adm_subj_admit` | `admissions` | subject_id, admittime | `NONUNIQUE` |
| `pk_admissions` | `admissions` | hadm_id | `UNIQUE` |
| `pk_caregiver` | `caregiver` | caregiver_id | `UNIQUE` |
| `idx_ce_stay_item_time` | `chartevents` | stay_id, itemid, charttime | `NONUNIQUE` |
| `idx_diag_aki_icd` | `diagnoses_icd` | icd_version, icd_code, hadm_id | `NONUNIQUE` |
| `idx_diag_hadm` | `diagnoses_icd` | hadm_id, icd_version, icd_code | `NONUNIQUE` |
| `idx_diag_hadm_code_norm` | `diagnoses_icd` | hadm_id, upper(replace(nvl(ICD_CODE,''),'.','')) | `NONUNIQUE` |
| `pk_d_icd_diagnoses` | `d_icd_diagnoses` | icd_code, icd_version | `UNIQUE` |
| `pk_d_icd_procedures` | `d_icd_procedures` | icd_code, icd_version | `UNIQUE` |
| `pk_d_items` | `d_items` | itemid | `UNIQUE` |
| `pk_d_labitems` | `d_labitems` | itemid | `UNIQUE` |
| `pk_emar` | `emar` | emar_id, emar_seq | `UNIQUE` |
| `idx_icu_hadm_stay` | `icustays` | hadm_id, stay_id | `NONUNIQUE` |
| `pk_icustays` | `icustays` | stay_id | `UNIQUE` |
| `idx_inpevents_stay_item_start` | `inputevents` | stay_id, itemid, starttime | `NONUNIQUE` |
| `idx_labevents_itemid_hadm_time` | `labevents` | itemid, hadm_id, charttime | `NONUNIQUE` |
| `pk_labevents` | `labevents` | labevent_id | `UNIQUE` |
| `idx_pat_age_gendern_subj` | `patients` | anchor_age, upper(trim(GENDER)), subject_id | `NONUNIQUE` |
| `pk_patients` | `patients` | subject_id | `UNIQUE` |
| `pk_poe` | `poe` | poe_id, poe_seq | `UNIQUE` |
| `idx_presc_subj_hadm_start` | `prescriptions` | subject_id, hadm_id, starttime | `NONUNIQUE` |
| `idx_procevents_stay_item_start` | `procedureevents` | stay_id, itemid, starttime | `NONUNIQUE` |
| `idx_proc_icd_hadm` | `procedures_icd` | icd_version, icd_code, hadm_id | `NONUNIQUE` |
| `pk_transfers` | `transfers` | transfer_id | `UNIQUE` |

## 4) 제약조건 정리

### 4.1 기본키 (PK)

| 제약조건명 | 테이블 | 컬럼 | 사용 인덱스 |
| --- | --- | --- | --- |
| `pk_admissions` | `admissions` | hadm_id | `pk_admissions` |
| `pk_caregiver` | `caregiver` | caregiver_id | `pk_caregiver` |
| `pk_d_icd_diagnoses` | `d_icd_diagnoses` | icd_code, icd_version | `pk_d_icd_diagnoses` |
| `pk_d_icd_procedures` | `d_icd_procedures` | icd_code, icd_version | `pk_d_icd_procedures` |
| `pk_d_items` | `d_items` | itemid | `pk_d_items` |
| `pk_d_labitems` | `d_labitems` | itemid | `pk_d_labitems` |
| `pk_emar` | `emar` | emar_id, emar_seq | `pk_emar` |
| `pk_icustays` | `icustays` | stay_id | `pk_icustays` |
| `pk_labevents` | `labevents` | labevent_id | `pk_labevents` |
| `pk_patients` | `patients` | subject_id | `pk_patients` |
| `pk_poe` | `poe` | poe_id, poe_seq | `pk_poe` |
| `pk_transfers` | `transfers` | transfer_id | `pk_transfers` |

### 4.2 외래키 (FK)

| 제약조건명 | 참조하는 컬럼 | 참조 대상 | 비고 |
| --- | --- | --- | --- |
| `fk_chartevents_subject` | `chartevents`.subject_id | `patients`.subject_id | - |
| `fk_dte_subject` | `datetimeevents`.subject_id | `patients`.subject_id | - |
| `fk_adm_subject` | `admissions`.subject_id | `patients`.subject_id | - |
| `fk_diag_hadm` | `diagnoses_icd`.hadm_id | `admissions`.hadm_id | - |
| `fk_diag_icd` | `diagnoses_icd`.icd_code, icd_version | `d_icd_diagnoses`.icd_code, icd_version | - |
| `fk_diag_subject` | `diagnoses_icd`.subject_id | `patients`.subject_id | - |
| `fk_emar_subject` | `emar`.subject_id | `patients`.subject_id | NOVALIDATE |
| `fk_icu_hadm` | `icustays`.hadm_id | `admissions`.hadm_id | - |
| `fk_icu_subject` | `icustays`.subject_id | `patients`.subject_id | - |
| `fk_labs_subject` | `labevents`.subject_id | `patients`.subject_id | - |
| `fk_presc_subject` | `prescriptions`.subject_id | `patients`.subject_id | - |
| `fk_proc_subject` | `procedures_icd`.subject_id | `patients`.subject_id | - |
| `fk_transfers_subject` | `transfers`.subject_id | `patients`.subject_id | - |

## 5) 구조 메모

- 환자 기준 허브 테이블은 `patients`이며, 다수의 임상 이벤트 테이블이 `subject_id`를 통해 참조합니다.
- 입원 기준 허브 테이블은 `admissions`이며, `diagnoses_icd`, `icustays` 등이 `hadm_id`로 연결됩니다.
- `emar`의 `fk_emar_subject`는 `NOVALIDATE`로 선언되어 기존 적재 데이터 전체 검증을 강제하지 않습니다.
- Function-based 인덱스(`idx_adm_los_expr`, `idx_diag_hadm_code_norm`, `idx_pat_age_gendern_subj`)를 포함해 조회 패턴 최적화가 반영되어 있습니다.
