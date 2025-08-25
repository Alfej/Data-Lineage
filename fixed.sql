

UPDATE DWL_P_BASE_WORK.PGT_FHVL_SLS_STG  SET DW_ERR = 'Y'
WHERE  (  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID ) IN
(
SELECT  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID
FROM DWL_P_BASE_WORK.PGT_FHVL_SLS_STG_stg
GROUP BY  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID HAVING COUNT(*) > 1
);

select * from '' as tbl
WHERE  (  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID ) IN
(
SELECT  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID
FROM DWL_P_BASE_WORK.PGT_FHVL_SLS_STG_stg
GROUP BY  DW_BLG_ID, INVC_LN_SEQ_NUM, RTE_DOC_DT, MANDT, SRC_SLS_TYP_CDV, SOLD_TO_CTRY_ISO_CDV, DW_CUST_ID HAVING COUNT(*) > 1
);

SEL CASE WHEN FLG_TYPE='I'
 THEN 
 'INDEX( '||COLUMNNAME||')'||' '||COALESCE(STATS_NM,'')||','
 WHEN FLG_TYPE='C'
 THEN 
'COLUMN('||COLUMNNAME||')'  ||' '||COALESCE(STATS_NM,'')||',' 
WHEN FLG_TYPE='NI'
 THEN 
 'INDEX  '||COLUMNNAME||''||' '||COALESCE(STATS_NM,'')||','
END FROM DWL_P_INTL.STATS_TBL
WHERE DATABASENAME='DWL_P_DRVD' AND TABLENAME='PGT_FLNA_SLS_CUST_MTRL_SUMMRY';


 insert into ACQ_P_JOB.pgt_sls_billing_hdr_err2
 (
  dw_step_id,
  dw_btch_id,
  ErrorCode,
  ErrorFieldName,
  DataParcel
 )
 select 
  this_step.mystep_id,
  this_step.mybtch_id,
  Null,
  'TPT_Reject',
 from (
  select max(actvty.cur_btch_id) as mybtch_id, max(step.step_id) as mystep_id
  from PEPCMN_P.step
  inner join PEPCMN_P.actvty
    on actvty.actvty_id = step.actvty_id
  inner join PEPCMN_P.sys
    on sys.sys_nm = actvty.sys_nm
  where
    sys.sys_nm = 'ACQ' and
    actvty_nm = 'PGT_SLS_BILLING_HDR' and
    step_nm = 'PGT_SLS_BILLING_HDR_LOAD'
 ) this_step ;

 update ACQ_P_WORK.fk_pgt_sls_billing_hdr_stg_CK_CUST_ID
  from ACQ_P.gtmd_ddh_cust_core_cf_key, '' as e
    set fk_CK_CUST_ID_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id and
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fk_CK_CUST_ID_surr is null;

 SELECT
                '<ExceptionRequest xmlns="http://www.PepsiCo.com/unique/default/namespace/CommonLE">
                <Header>
                <ApplicationID>ACQ</ApplicationID>
                <ServiceName>ACQ_LOADER</ServiceName>
                <ComponentName>SOURCE_TO_CORE</ComponentName>
                <Hostname>peplap00726</Hostname>
                <Timestamp>2025-08-07T22:20:48</Timestamp>
                </Header>
                <Category>LOAD_ERROR</Category>
                <Type>'||UPPER('pgt')||'</Type>
                <Severity>2</Severity>
                <Code>N/A</Code>
                <Message>Error Records: '||TRIM(ERR_COUNT)||' Error table: pgt_sls_billing_hdr_err</Message>
                <DumpAnalysis>&#xD----------------- &#xD SYSTEM: ACQ  ACTIVITY: PGT_SLS_BILLING_HDR  STEP: PGT_SLS_BILLING_HDR_LOAD  TABLE: pgt_sls_billing_hdr_err&#xDSTEP_ID: '||DW_STEP_ID||'  BATCH_ID: '||DW_BTCH_ID||'&#xDQUERY: select * from ACQ_P_JOB.pgt_sls_billing_hdr_err where DW_BTCH_ID in ('||DW_BTCH_ID||')&#xDWeb Link: https://ews.mypepsico.com/acq_err/?acq_table=pgt_sls_billing_hdr_err&ampacq_max=100&ampacq_date=&ampaction=View&ampmode=rejected&ampacq_batch='||DW_BTCH_ID||'</DumpAnalysis>
                </ExceptionRequest>'
        FROM
                (       SELECT
                                TRIM(DW_BTCH_ID(INTEGER)) AS DW_BTCH_ID, 
        TRIM(DW_STEP_ID(INTEGER)) AS DW_STEP_ID,
        COUNT(*) as ERR_COUNT
                        FROM
                                ACQ_P_JOB.pgt_sls_billing_hdr_err ERR
                                JOIN PEPCMN_P.ACTVTY A
                                        ON ERR.DW_BTCH_ID = A.CUR_BTCH_ID
                                JOIN PEPCMN_P.STEP S
                                        ON A.ACTVTY_ID=S.ACTVTY_ID
                                        AND ERR.DW_STEP_ID=S.STEP_ID
                        WHERE
                                SYS_NM='ACQ'
                                AND ACTVTY_NM='PGT_SLS_BILLING_HDR'
                        GROUP BY 1,2
                        HAVING ERR_COUNT>0
                )A;


    insert into MYDAT_ERR_EVNT 
  (
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    ERR_CD, 
    OPTNL_RCRD_NM, 
    OPTNL_RCRD_ID, 
    OPTNL_FLD_NM, 
    OPTNL_FLD_VAL, 
    ERR_SVRTY, 
    ERR_DTL_1, 
    ERR_DTL_2
  )
  select 
    tbl.blg_doc_id,
    tbl.blg_ln_num,
    tbl.mandt,
    tbl.zsysid,
    'FLD_NULL',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARCHAR(1000)),
    'blg_doc_id',
    blg_doc_id,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where blg_doc_id is null;
