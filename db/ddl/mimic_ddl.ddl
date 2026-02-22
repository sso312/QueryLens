
CREATE TABLESPACE team9_tbs 
--  WARNING: Tablespace has no data files defined 
 LOGGING ONLINE
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
FLASHBACK ON;

CREATE ROLE team9_user NOT IDENTIFIED;

CREATE USER sso IDENTIFIED BY ACCOUNT UNLOCK ;

-- predefined type, no DDL - MDSYS.SDO_GEOMETRY

-- predefined type, no DDL - XMLTYPE

CREATE TABLE sso.admissions (
    subject_id           NUMBER(10) NOT NULL,
    hadm_id              NUMBER(10) NOT NULL,
    admittime            TIMESTAMP NOT NULL,
    dischtime            TIMESTAMP,
    deathtime            TIMESTAMP,
    admission_type       VARCHAR2(40 BYTE) NOT NULL,
    admit_provider_id    VARCHAR2(10 BYTE),
    admission_location   VARCHAR2(60 BYTE),
    discharge_location   VARCHAR2(60 BYTE),
    insurance            VARCHAR2(255 BYTE),
    language             VARCHAR2(30 BYTE),
    marital_status       VARCHAR2(30 BYTE),
    race                 VARCHAR2(80 BYTE),
    edregtime            TIMESTAMP,
    edouttime            TIMESTAMP,
    hospital_expire_flag NUMBER(1)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

CREATE UNIQUE INDEX sso.pk_admissions ON
    sso.admissions (
        hadm_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.admissions TO team9_user;

ALTER TABLE sso.admissions
    ADD CONSTRAINT pk_admissions PRIMARY KEY ( hadm_id )
        USING INDEX sso.pk_admissions;

CREATE TABLE sso.caregiver (
    caregiver_id VARCHAR2(10 BYTE) NOT NULL
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.caregiver TO team9_user;

CREATE TABLE sso.chartevents (
    subject_id   NUMBER(10) NOT NULL,
    hadm_id      NUMBER(10),
    stay_id      NUMBER(10),
    caregiver_id NUMBER(10),
    charttime    TIMESTAMP(0),
    storetime    TIMESTAMP(0),
    itemid       NUMBER(10) NOT NULL,
    value        VARCHAR2(200 CHAR),
    valuenum     BINARY_DOUBLE,
    valueuom     VARCHAR2(20 CHAR),
    warning      NUMBER(5)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs LOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.chartevents TO team9_user;

CREATE TABLE sso.d_icd_diagnoses (
    icd_code    CHAR(7 BYTE) NOT NULL,
    icd_version NUMBER(2) NOT NULL,
    long_title  VARCHAR2(1000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

CREATE UNIQUE INDEX sso.pk_d_icd_diagnoses ON
    sso.d_icd_diagnoses (
        icd_code
    ASC,
        icd_version
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.d_icd_diagnoses TO team9_user;

ALTER TABLE sso.d_icd_diagnoses
    ADD CONSTRAINT pk_d_icd_diagnoses PRIMARY KEY ( icd_code,
                                                    icd_version )
        USING INDEX sso.pk_d_icd_diagnoses;

CREATE TABLE sso.d_icd_procedures (
    icd_code    CHAR(7 BYTE) NOT NULL,
    icd_version NUMBER(2) NOT NULL,
    long_title  VARCHAR2(1000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

CREATE UNIQUE INDEX sso.pk_d_icd_procedures ON
    sso.d_icd_procedures (
        icd_code
    ASC,
        icd_version
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.d_icd_procedures TO team9_user;

ALTER TABLE sso.d_icd_procedures
    ADD CONSTRAINT pk_d_icd_procedures PRIMARY KEY ( icd_code,
                                                     icd_version )
        USING INDEX sso.pk_d_icd_procedures;

CREATE TABLE sso.d_items (
    itemid          NUMBER(10),
    label           VARCHAR2(200 BYTE),
    abbreviation    VARCHAR2(100 BYTE),
    linksto         VARCHAR2(50 BYTE),
    category        VARCHAR2(100 BYTE),
    unitname        VARCHAR2(100 BYTE),
    param_type      VARCHAR2(30 BYTE),
    lownormalvalue  NUMBER,
    highnormalvalue NUMBER
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.d_items TO team9_user;

CREATE TABLE sso.d_labitems (
    itemid   NUMBER(10) NOT NULL,
    label    VARCHAR2(100 CHAR),
    fluid    VARCHAR2(50 CHAR),
    category VARCHAR2(50 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.d_labitems TO team9_user;

CREATE TABLE sso.datetimeevents (
    subject_id   NUMBER(10),
    hadm_id      NUMBER(10),
    stay_id      NUMBER(10),
    caregiver_id NUMBER(10),
    charttime    TIMESTAMP(3),
    storetime    TIMESTAMP(3),
    itemid       NUMBER(10),
    value        TIMESTAMP(3),
    valueuom     VARCHAR2(20 BYTE),
    warning      NUMBER(5)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.datetimeevents TO team9_user;

CREATE TABLE sso.diagnoses_icd (
    subject_id  NUMBER(10) NOT NULL,
    hadm_id     NUMBER(10) NOT NULL,
    seq_num     NUMBER(5) NOT NULL,
    icd_code    CHAR(7 BYTE),
    icd_version NUMBER(2)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.diagnoses_icd TO team9_user;

CREATE TABLE sso.emar (
    subject_id        NUMBER(10) NOT NULL,
    hadm_id           NUMBER(10),
    emar_id           VARCHAR2(25 BYTE) NOT NULL,
    emar_seq          NUMBER(10) NOT NULL,
    poe_id            VARCHAR2(25 BYTE) NOT NULL,
    pharmacy_id       NUMBER(10),
    enter_provider_id VARCHAR2(10 BYTE),
    charttime         TIMESTAMP(0) NOT NULL,
    event_txt         VARCHAR2(100 BYTE),
    scheduletime      TIMESTAMP(0),
    storetime         TIMESTAMP(0) NOT NULL,
    medication        VARCHAR2(1000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

CREATE UNIQUE INDEX sso.pk_emar ON
    sso.emar (
        emar_id
    ASC,
        emar_seq
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.emar TO team9_user;

ALTER TABLE sso.emar
    ADD CONSTRAINT pk_emar PRIMARY KEY ( emar_id,
                                         emar_seq )
        USING INDEX sso.pk_emar;

CREATE TABLE sso.emar_detail (
    subject_id                           NUMBER(10) NOT NULL,
    emar_id                              VARCHAR2(25 BYTE) NOT NULL,
    emar_seq                             NUMBER(10) NOT NULL,
    parent_field_ordinal                 VARCHAR2(10 BYTE),
    administration_type                  VARCHAR2(50 BYTE),
    pharmacy_id                          NUMBER(10),
    barcode_type                         VARCHAR2(4 BYTE),
    complete_dose_not_given              VARCHAR2(5 BYTE),
    dose_due                             VARCHAR2(100 BYTE),
    dose_due_unit                        VARCHAR2(50 BYTE),
    dose_given                           VARCHAR2(255 BYTE),
    dose_given_unit                      VARCHAR2(50 BYTE), 
--  ERROR: Column name length exceeds maximum allowed length(30) 
    will_remainder_of_dose_be_given      VARCHAR2(5 BYTE),
    product_amount_given                 VARCHAR2(30 BYTE),
    product_unit                         VARCHAR2(30 BYTE),
    product_code                         VARCHAR2(30 BYTE),
    product_description                  VARCHAR2(255 BYTE),
    product_description_other            VARCHAR2(255 BYTE),
    prior_infusion_rate                  VARCHAR2(40 BYTE),
    infusion_rate                        VARCHAR2(40 BYTE),
    infusion_rate_adjustment             VARCHAR2(50 BYTE), 
--  ERROR: Column name length exceeds maximum allowed length(30) 
    infusion_rate_adjustment_amount      VARCHAR2(30 BYTE),
    infusion_rate_unit                   VARCHAR2(30 BYTE),
    route                                VARCHAR2(10 BYTE),
    infusion_complete                    VARCHAR2(1 BYTE),
    completion_interval                  VARCHAR2(50 BYTE),
    new_iv_bag_hung                      VARCHAR2(1 BYTE), 
--  ERROR: Column name length exceeds maximum allowed length(30) 
    continued_infusion_in_other_location VARCHAR2(1 BYTE),
    side                                 VARCHAR2(10 BYTE),
    site                                 VARCHAR2(255 BYTE), 
--  ERROR: Column name length exceeds maximum allowed length(30) 
    non_formulary_visual_verification    VARCHAR2(1 BYTE),
    reason_for_no_barcode                VARCHAR2(1000 CHAR),
    restart_interval                     VARCHAR2(1000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.emar_detail TO team9_user;

CREATE TABLE sso.icustays (
    subject_id     NUMBER(10) NOT NULL,
    hadm_id        NUMBER(10) NOT NULL,
    stay_id        NUMBER(10) NOT NULL,
    first_careunit VARCHAR2(50 BYTE),
    last_careunit  VARCHAR2(50 BYTE),
    intime         TIMESTAMP(0),
    outtime        TIMESTAMP(0),
    los            NUMBER(10, 4)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

CREATE UNIQUE INDEX sso.pk_icustays ON
    sso.icustays (
        stay_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.icustays TO team9_user;

ALTER TABLE sso.icustays
    ADD CONSTRAINT pk_icustays PRIMARY KEY ( stay_id )
        USING INDEX sso.pk_icustays;

CREATE TABLE sso.ingredientevents (
    subject_id        NUMBER(10),
    hadm_id           NUMBER(10),
    stay_id           NUMBER(10),
    caregiver_id      NUMBER(10),
    starttime         TIMESTAMP(0),
    endtime           TIMESTAMP(0),
    storetime         TIMESTAMP(0),
    itemid            NUMBER(10),
    amount            NUMBER,
    amountuom         VARCHAR2(20 BYTE),
    rate              NUMBER,
    rateuom           VARCHAR2(20 BYTE),
    orderid           NUMBER(10),
    linkorderid       NUMBER(10),
    statusdescription VARCHAR2(20 BYTE),
    originalamount    NUMBER,
    originalrate      NUMBER
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.ingredientevents TO team9_user;

CREATE TABLE sso.inputevents (
    subject_id     NUMBER(10) NOT NULL,
    hadm_id        NUMBER(10),
    stay_id        NUMBER(10),
    starttime      TIMESTAMP(0),
    endtime        TIMESTAMP(0),
    itemid         NUMBER(10) NOT NULL,
    amount         BINARY_DOUBLE,
    amountuom      VARCHAR2(30 CHAR),
    rate           BINARY_DOUBLE,
    rateuom        VARCHAR2(30 CHAR),
    orderid        NUMBER(10),
    linkorderid    NUMBER(10),
    stopped        VARCHAR2(20 CHAR),
    originalamount BINARY_DOUBLE,
    originalrate   BINARY_DOUBLE
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.inputevents TO team9_user;

CREATE TABLE sso.labevents (
    labevent_id       NUMBER(10) NOT NULL,
    subject_id        NUMBER(10) NOT NULL,
    hadm_id           NUMBER(10),
    specimen_id       NUMBER(10) NOT NULL,
    itemid            NUMBER(10) NOT NULL,
    order_provider_id VARCHAR2(10 CHAR),
    charttime         TIMESTAMP(0),
    storetime         TIMESTAMP(0),
    value             VARCHAR2(200 CHAR),
    valuenum          BINARY_DOUBLE,
    valueuom          VARCHAR2(20 CHAR),
    ref_range_lower   BINARY_DOUBLE,
    ref_range_upper   BINARY_DOUBLE,
    flag              VARCHAR2(10 CHAR),
    priority          VARCHAR2(7 CHAR),
    comments          VARCHAR2(4000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

CREATE UNIQUE INDEX sso.pk_labevents ON
    sso.labevents (
        labevent_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.labevents TO team9_user;

ALTER TABLE sso.labevents
    ADD CONSTRAINT pk_labevents PRIMARY KEY ( labevent_id )
        USING INDEX sso.pk_labevents;

CREATE TABLE sso.microbiologyevents (
    microevent_id       NUMBER(10) NOT NULL,
    subject_id          NUMBER(10) NOT NULL,
    hadm_id             NUMBER(10),
    micro_specimen_id   NUMBER(10) NOT NULL,
    order_provider_id   VARCHAR2(10 BYTE),
    chartdate           TIMESTAMP(0) NOT NULL,
    charttime           TIMESTAMP(0),
    spec_itemid         NUMBER(10) NOT NULL,
    spec_type_desc      VARCHAR2(100 BYTE) NOT NULL,
    test_seq            NUMBER(10) NOT NULL,
    storedate           TIMESTAMP(0),
    storetime           TIMESTAMP(0),
    test_itemid         NUMBER(10),
    test_name           VARCHAR2(100 BYTE),
    org_itemid          NUMBER(10),
    org_name            VARCHAR2(100 BYTE),
    isolate_num         NUMBER(5),
    quantity            VARCHAR2(50 BYTE),
    ab_itemid           NUMBER(10),
    ab_name             VARCHAR2(30 BYTE),
    dilution_text       VARCHAR2(10 BYTE),
    dilution_comparison VARCHAR2(20 BYTE),
    dilution_value      NUMBER,
    interpretation      VARCHAR2(5 BYTE),
    comments            VARCHAR2(4000 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.microbiologyevents TO team9_user;

CREATE TABLE sso.outputevents (
    subject_id   NUMBER(10) NOT NULL,
    hadm_id      NUMBER(10),
    stay_id      NUMBER(10),
    caregiver_id NUMBER(10),
    charttime    TIMESTAMP(3),
    storetime    TIMESTAMP(3),
    itemid       NUMBER(10) NOT NULL,
    value        BINARY_DOUBLE,
    valueuom     VARCHAR2(30 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.outputevents TO team9_user;

CREATE TABLE sso.patients (
    subject_id        NUMBER(10) NOT NULL,
    gender            CHAR(1 BYTE) NOT NULL,
    anchor_age        NUMBER(3) NOT NULL,
    anchor_year       NUMBER(4) NOT NULL,
    anchor_year_group VARCHAR2(255 BYTE) NOT NULL,
    dod               DATE
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

CREATE UNIQUE INDEX sso.pk_patients ON
    sso.patients (
        subject_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.patients TO team9_user;

ALTER TABLE sso.patients
    ADD CONSTRAINT pk_patients PRIMARY KEY ( subject_id )
        USING INDEX sso.pk_patients;

CREATE TABLE sso.poe (
    poe_id                 VARCHAR2(25 BYTE) NOT NULL,
    poe_seq                NUMBER(10) NOT NULL,
    subject_id             NUMBER(10) NOT NULL,
    hadm_id                NUMBER(10),
    ordertime              TIMESTAMP(0) NOT NULL,
    order_type             VARCHAR2(25 BYTE) NOT NULL,
    order_subtype          VARCHAR2(50 BYTE),
    transaction_type       VARCHAR2(15 BYTE),
    discontinue_of_poe_id  VARCHAR2(25 BYTE),
    discontinued_by_poe_id VARCHAR2(25 BYTE),
    order_provider_id      VARCHAR2(10 BYTE),
    order_status           VARCHAR2(15 BYTE)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.poe TO team9_user;

CREATE TABLE sso.prescriptions (
    subject_id        NUMBER(10) NOT NULL,
    hadm_id           NUMBER(10) NOT NULL,
    pharmacy_id       NUMBER(10) NOT NULL,
    poe_id            VARCHAR2(50 CHAR),
    poe_seq           NUMBER(5),
    order_provider_id VARCHAR2(30 CHAR),
    starttime         TIMESTAMP(3),
    stoptime          TIMESTAMP(3),
    drug_type         VARCHAR2(50 CHAR) NOT NULL,
    drug              VARCHAR2(1000 CHAR) NOT NULL,
    formulary_drug_cd VARCHAR2(100 CHAR),
    gsn               VARCHAR2(255 CHAR),
    ndc               VARCHAR2(50 CHAR),
    prod_strength     VARCHAR2(255 CHAR),
    form_rx           VARCHAR2(50 CHAR),
    dose_val_rx       VARCHAR2(200 CHAR),
    dose_unit_rx      VARCHAR2(100 CHAR),
    form_val_disp     VARCHAR2(100 CHAR),
    form_unit_disp    VARCHAR2(100 CHAR),
    doses_per_24_hrs  BINARY_FLOAT,
    route             VARCHAR2(100 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.prescriptions TO team9_user;

CREATE TABLE sso.procedureevents (
    subject_id               NUMBER(10) NOT NULL,
    hadm_id                  NUMBER(10),
    stay_id                  NUMBER(10),
    caregiver_id             NUMBER(10),
    starttime                TIMESTAMP(0),
    endtime                  TIMESTAMP(0),
    storetime                TIMESTAMP(0),
    itemid                   NUMBER(10) NOT NULL,
    value                    BINARY_DOUBLE,
    valueuom                 VARCHAR2(30 CHAR),
    location                 VARCHAR2(100 CHAR),
    locationcategory         VARCHAR2(50 CHAR),
    orderid                  NUMBER(10),
    linkorderid              NUMBER(10),
    ordercategoryname        VARCHAR2(50 CHAR),
    ordercategorydescription VARCHAR2(50 CHAR),
    patientweight            BINARY_DOUBLE,
    isopenbag                NUMBER(1),
    continueinnextdept       NUMBER(1),
    statusdescription        VARCHAR2(30 CHAR),
    originalamount           BINARY_DOUBLE,
    originalrate             BINARY_DOUBLE
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY
    ENABLE ROW MOVEMENT;

GRANT SELECT ON sso.procedureevents TO team9_user;

CREATE TABLE sso.procedures_icd (
    subject_id  NUMBER(10) NOT NULL,
    hadm_id     NUMBER(10) NOT NULL,
    seq_num     NUMBER(5) NOT NULL,
    chartdate   DATE NOT NULL,
    icd_code    VARCHAR2(7 CHAR),
    icd_version NUMBER(2)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.procedures_icd TO team9_user;

CREATE TABLE sso.services (
    subject_id   NUMBER(10) NOT NULL,
    hadm_id      NUMBER(10) NOT NULL,
    transfertime TIMESTAMP(0),
    prev_service VARCHAR2(20 CHAR),
    curr_service VARCHAR2(20 CHAR)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

GRANT SELECT ON sso.services TO team9_user;

CREATE TABLE sso.transfers (
    subject_id  NUMBER(10) NOT NULL,
    hadm_id     NUMBER(10),
    transfer_id NUMBER(10) NOT NULL,
    eventtype   VARCHAR2(20 CHAR),
    careunit    VARCHAR2(255 CHAR),
    intime      TIMESTAMP(0),
    outtime     TIMESTAMP(0)
)
PCTFREE 10 PCTUSED 40 TABLESPACE team9_tbs NOLOGGING
    STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
    DEFAULT )
NO INMEMORY;

CREATE UNIQUE INDEX sso.pk_transfers ON
    sso.transfers (
        transfer_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

GRANT SELECT ON sso.transfers TO team9_user;

ALTER TABLE sso.transfers
    ADD CONSTRAINT pk_transfers PRIMARY KEY ( transfer_id )
        USING INDEX sso.pk_transfers;

CREATE INDEX sso.idx_adm_los_expr ON
    sso.admissions ( CAST(
        "DISCHTIME"
    AS DATE) - CAST(
        "ADMITTIME"
    AS DATE) )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_adm_subj_admit ON
    sso.admissions (
        subject_id
    ASC,
        admittime
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE UNIQUE INDEX sso.pk_caregiver ON
    sso.caregiver (
        caregiver_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

ALTER TABLE sso.caregiver
    ADD CONSTRAINT pk_caregiver PRIMARY KEY ( caregiver_id )
        USING INDEX sso.pk_caregiver;

CREATE INDEX sso.idx_ce_stay_item_time ON
    sso.chartevents (
        stay_id
    ASC,
        itemid
    ASC,
        charttime
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_diag_aki_icd ON
    sso.diagnoses_icd (
        icd_version
    ASC,
        icd_code
    ASC,
        hadm_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_diag_hadm ON
    sso.diagnoses_icd (
        hadm_id
    ASC,
        icd_version
    ASC,
        icd_code
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_diag_hadm_code_norm ON
    sso.diagnoses_icd (
        hadm_id,
        upper(replace(nvl("ICD_CODE", ''), '.', ''))
    )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE UNIQUE INDEX sso.pk_d_items ON
    sso.d_items (
        itemid
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

ALTER TABLE sso.d_items
    ADD CONSTRAINT pk_d_items PRIMARY KEY ( itemid )
        USING INDEX sso.pk_d_items;

CREATE UNIQUE INDEX sso.pk_d_labitems ON
    sso.d_labitems (
        itemid
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

ALTER TABLE sso.d_labitems
    ADD CONSTRAINT pk_d_labitems PRIMARY KEY ( itemid )
        USING INDEX sso.pk_d_labitems;

CREATE INDEX sso.idx_icu_hadm_stay ON
    sso.icustays (
        hadm_id
    ASC,
        stay_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_inpevents_stay_item_start ON
    sso.inputevents (
        stay_id
    ASC,
        itemid
    ASC,
        starttime
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_labevents_itemid_hadm_time ON
    sso.labevents (
        itemid
    ASC,
        hadm_id
    ASC,
        charttime
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_pat_age_gendern_subj ON
    sso.patients (
        anchor_age,
        upper(TRIM("GENDER")),
        subject_id
    )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE UNIQUE INDEX sso.pk_poe ON
    sso.poe (
        poe_id
    ASC,
        poe_seq
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

ALTER TABLE sso.poe
    ADD CONSTRAINT pk_poe PRIMARY KEY ( poe_id,
                                        poe_seq )
        USING INDEX sso.pk_poe;

CREATE INDEX sso.idx_presc_subj_hadm_start ON
    sso.prescriptions (
        subject_id
    ASC,
        hadm_id
    ASC,
        starttime
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_procevents_stay_item_start ON
    sso.procedureevents (
        stay_id,
        itemid,
        starttime
    )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

CREATE INDEX sso.idx_proc_icd_hadm ON
    sso.procedures_icd (
        icd_version
    ASC,
        icd_code
    ASC,
        hadm_id
    ASC )
        TABLESPACE team9_tbs PCTFREE 10
            STORAGE ( INITIAL 1048576 NEXT 1048576 PCTINCREASE 0 MINEXTENTS 1 MAXEXTENTS 2147483645 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL
            DEFAULT )
        LOGGING;

ALTER TABLE sso.chartevents
    ADD CONSTRAINT fk_chartevents_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.datetimeevents
    ADD CONSTRAINT fk_dte_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.admissions
    ADD CONSTRAINT fk_adm_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.diagnoses_icd
    ADD CONSTRAINT fk_diag_hadm FOREIGN KEY ( hadm_id )
        REFERENCES sso.admissions ( hadm_id )
    NOT DEFERRABLE;

ALTER TABLE sso.diagnoses_icd
    ADD CONSTRAINT fk_diag_icd FOREIGN KEY ( icd_code,
                                             icd_version )
        REFERENCES sso.d_icd_diagnoses ( icd_code,
                                         icd_version )
    NOT DEFERRABLE;

ALTER TABLE sso.diagnoses_icd
    ADD CONSTRAINT fk_diag_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.emar
    ADD CONSTRAINT fk_emar_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE NOVALIDATE;

ALTER TABLE sso.icustays
    ADD CONSTRAINT fk_icu_hadm FOREIGN KEY ( hadm_id )
        REFERENCES sso.admissions ( hadm_id )
    NOT DEFERRABLE;

ALTER TABLE sso.icustays
    ADD CONSTRAINT fk_icu_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.labevents
    ADD CONSTRAINT fk_labs_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.prescriptions
    ADD CONSTRAINT fk_presc_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.procedures_icd
    ADD CONSTRAINT fk_proc_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;

ALTER TABLE sso.transfers
    ADD CONSTRAINT fk_transfers_subject FOREIGN KEY ( subject_id )
        REFERENCES sso.patients ( subject_id )
    NOT DEFERRABLE;



-- Oracle SQL Developer Data Modeler 
-- 
-- CREATE TABLE                            24
-- CREATE INDEX                            25
-- ALTER TABLE                             25
-- CREATE VIEW                              0
-- ALTER VIEW                               0
-- CREATE PACKAGE                           0
-- CREATE PACKAGE BODY                      0
-- CREATE PROCEDURE                         0
-- CREATE FUNCTION                          0
-- CREATE TRIGGER                           0
-- ALTER TRIGGER                            0
-- CREATE COLLECTION TYPE                   0
-- CREATE STRUCTURED TYPE                   0
-- CREATE STRUCTURED TYPE BODY              0
-- CREATE CLUSTER                           0
-- CREATE CONTEXT                           0
-- CREATE DATABASE                          0
-- CREATE DIMENSION                         0
-- CREATE DIRECTORY                         0
-- CREATE DISK GROUP                        0
-- CREATE ROLE                              1
-- CREATE ROLLBACK SEGMENT                  0
-- CREATE SEQUENCE                          0
-- CREATE MATERIALIZED VIEW                 0
-- CREATE MATERIALIZED VIEW LOG             0
-- CREATE SYNONYM                           0
-- CREATE TABLESPACE                        1
-- CREATE USER                              1
-- 
-- DROP TABLESPACE                          0
-- DROP DATABASE                            0
-- 
-- REDACTION POLICY                         0
-- 
-- ORDS DROP SCHEMA                         0
-- ORDS ENABLE SCHEMA                       0
-- ORDS ENABLE OBJECT                       0
-- 
-- ERRORS                                   4
-- WARNINGS                                 1
