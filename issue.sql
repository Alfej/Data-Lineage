UPDATE DWL_P_BASE_WORK.PGT_FHVL_SLS_STG  SET DW_ERR = 'Y'
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
END  (TITLE '') FROM DWL_P_INTL.STATS_TBL
WHERE DATABASENAME='DWL_P_DRVD' AND TABLENAME='PGT_FLNA_SLS_CUST_MTRL_SUMMRY';

  select count(*) (TITLE '')
  from ACQ_P_UTIL.pgt_sls_billing_hdr_stg_et;

 USING F1 (Varbyte(63500))
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
  :F1
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

   select count(*) (TITLE '')
  from ACQ_P_UTIL.pgt_sls_billing_hdr_stg_uv;

  CREATE MULTISET VOLATILE TABLE MYDAT_ERR_EVNT, 
     NO FALLBACK,
     CHECKSUM = DEFAULT,
     NO LOG
     (
      blg_doc_id varchar(10) character set unicode  ,
      mandt varchar(3) character set unicode  ,
      zsysid varchar(8) character set unicode  ,
      ERR_CD VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      OPTNL_RCRD_NM VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_RCRD_ID VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_FLD_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_FLD_VAL VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_SVRTY VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_DTL_1 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_DTL_2 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC
     )
  PRIMARY INDEX ( blg_doc_id, mandt, zsysid )
  ON COMMIT PRESERVE ROWS;

    update ACQ_P_WORK.fk_pgt_sls_billing_hdr_stg_CK_CUST_ID
  from ACQ_P.gtmd_ddh_cust_core_cf_key
    set fk_CK_CUST_ID_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id and
    fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_hdr_stg_CK_CUST_ID.fk_CK_CUST_ID_surr is null;

  update ACQ_P_WORK.fk_pgt_sls_billing_hdr_stg_CK_SHIP_TO_CUST_ID
  from ACQ_P.gtmd_ddh_cust_core_cf_key
    set fk_CK_SHIP_TO_CUST_ID_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_hdr_stg_CK_SHIP_TO_CUST_ID.fknk_ship_to_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_hdr_stg_CK_SHIP_TO_CUST_ID.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id and
    fk_pgt_sls_billing_hdr_stg_CK_SHIP_TO_CUST_ID.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_hdr_stg_CK_SHIP_TO_CUST_ID.fk_CK_SHIP_TO_CUST_ID_surr is null;


  update ACQ_P_WORK.fk_pgt_sls_billing_hdr_stg_LK_SLS_LOC_ID
  from ACQ_P.gtmd_ddh_loc_core_cf_key
    set fk_LK_SLS_LOC_ID_surr = gtmd_ddh_loc_core_cf_key.dw_loc_id
  where
    fk_pgt_sls_billing_hdr_stg_LK_SLS_LOC_ID.fknk_sls_loc_id = gtmd_ddh_loc_core_cf_key.loc_id and
    fk_pgt_sls_billing_hdr_stg_LK_SLS_LOC_ID.fknk_mandt = gtmd_ddh_loc_core_cf_key.client_id and
    fk_pgt_sls_billing_hdr_stg_LK_SLS_LOC_ID.fknk_acq_pgt_sys_id = gtmd_ddh_loc_core_cf_key.pgt_sys_id
    and gtmd_ddh_loc_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_hdr_stg_LK_SLS_LOC_ID.fk_LK_SLS_LOC_ID_surr is null;

  select
    err_cd || '|' ||
    err_svrty || '|' ||
    cast(total as varchar(100))  (TITLE '')
  from
  (select 
    err_cd, 
    err_svrty, 
    count(*) as total
  from MYDAT_ERR_EVNT
  group by 1,2 
  UNION ALL
  select 
    'ALL',
    err_svrty, 
    count(*) as total
  from MYDAT_ERR_EVNT
  group by 2) tbl;

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
                <Message>Error Records: '||TRIM(ERR_COUNT)||'; Error table: pgt_sls_billing_hdr_err</Message>
                <DumpAnalysis>&#xD;----------------- &#xD; SYSTEM: ACQ;  ACTIVITY: PGT_SLS_BILLING_HDR;  STEP: PGT_SLS_BILLING_HDR_LOAD;  TABLE: pgt_sls_billing_hdr_err;&#xD;STEP_ID: '||DW_STEP_ID||';  BATCH_ID: '||DW_BTCH_ID||';&#xD;QUERY: select * from ACQ_P_JOB.pgt_sls_billing_hdr_err where DW_BTCH_ID in ('||DW_BTCH_ID||');&#xD;Web Link: https://ews.mypepsico.com/acq_err/?acq_table=pgt_sls_billing_hdr_err&amp;acq_max=100&amp;acq_date=&amp;action=View&amp;mode=rejected&amp;acq_batch='||DW_BTCH_ID||'</DumpAnalysis>
                </ExceptionRequest>' (TITLE '')
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
                <Message>Error Records: '||TRIM(ERR_COUNT)||'; Error table: pgt_sls_billing_hdr_err2</Message>
				<DumpAnalysis>&#xD;-----------------&#xD;SYSTEM: ACQ;  ACTIVITY: PGT_SLS_BILLING_HDR;  STEP: PGT_SLS_BILLING_HDR_LOAD;  TABLE: pgt_sls_billing_hdr_err;&#xD;STEP_ID: '||DW_STEP_ID||';  BATCH_ID: '||DW_BTCH_ID||';&#xD;QUERY: select * from ACQ_P_JOB.pgt_sls_billing_hdr_err2 where DW_BTCH_ID in ('||DW_BTCH_ID||');&#xD;Web Link: https://ews.mypepsico.com/acq_err/?acq_table=pgt_sls_billing_hdr_err2&amp;acq_max=100&amp;acq_date=&amp;action=View&amp;mode=rejected&amp;acq_batch='||DW_BTCH_ID||'</DumpAnalysis>
                </ExceptionRequest>' (TITLE '')
        FROM
                (       SELECT
                                TRIM(DW_BTCH_ID(INTEGER)) AS DW_BTCH_ID, 
        TRIM(DW_STEP_ID(INTEGER)) AS DW_STEP_ID,
        COUNT(*) as ERR_COUNT
                        FROM
                                ACQ_P_JOB.pgt_sls_billing_hdr_err2 ERR
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


  select count(*) (TITLE '')
  from ACQ_P_UTIL.pgt_sls_billing_item_stg_et;

  
 USING F1 (Varbyte(63500))
 insert into ACQ_P_JOB.pgt_sls_billing_item_err2
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
  :F1
 from (
  select max(actvty.cur_btch_id) as mybtch_id, max(step.step_id) as mystep_id
  from PEPCMN_P.step
  inner join PEPCMN_P.actvty
    on actvty.actvty_id = step.actvty_id
  inner join PEPCMN_P.sys
    on sys.sys_nm = actvty.sys_nm
  where
    sys.sys_nm = 'ACQ' and
    actvty_nm = 'PGT_SLS_BILLING_ITEM' and
    step_nm = 'PGT_SLS_BILLING_ITEM_LOAD'
 ) this_step ;

   select count(*) (TITLE '')
  from ACQ_P_UTIL.pgt_sls_billing_item_stg_uv;

    CREATE MULTISET VOLATILE TABLE MYDAT_ERR_EVNT, 
     NO FALLBACK,
     CHECKSUM = DEFAULT,
     NO LOG
     (
      blg_doc_id varchar(10) character set unicode  ,
      blg_ln_num varchar(6) character set unicode  ,
      mandt varchar(3) character set unicode  ,
      zsysid varchar(8) character set unicode  ,
      ERR_CD VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      OPTNL_RCRD_NM VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_RCRD_ID VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_FLD_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OPTNL_FLD_VAL VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_SVRTY VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_DTL_1 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ERR_DTL_2 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC
     )
  PRIMARY INDEX ( blg_doc_id, blg_ln_num, mandt, zsysid )
  ON COMMIT PRESERVE ROWS;

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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'blg_doc_id',
    blg_doc_id,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where blg_doc_id is null;


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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'blg_ln_num',
    blg_ln_num,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where blg_ln_num is null;


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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'mandt',
    mandt,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where mandt is null;


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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'zsysid',
    zsysid,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where zsysid is null;


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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'acq_mdm_sys_id',
    acq_mdm_sys_id,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where acq_mdm_sys_id is null
  ;


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
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'acq_pgt_sys_id',
    acq_pgt_sys_id,
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from ACQ_P_STAGE.pgt_sls_billing_item_stg as tbl where acq_pgt_sys_id is null
  ;


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
    'RCD_DUPLICATE',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'all',
    'all',
    'WARN',
    'pgt_sls_billing_item_stg',
    null
    from 
  (select 
    acq_mdm_sys_id,
    acq_pgt_sys_id,
 autyp,
    bill_to_cust_id,
    blg_cndtn_doc_id_val,
    blg_cndtn_typ_cdv,
    blg_crncy_cdv,
    blg_doc_dt,
    blg_doc_id,
    blg_ln_adjmt_amt,
    blg_ln_adjmt_frgnccy_amt,
    blg_ln_ctgy_cdv,
    blg_ln_grss_amt,
    blg_ln_grss_frgnccy_amt,
    blg_ln_grss_uprc_amt,
    blg_ln_grss_uprc_frgnccy_amt,
    blg_ln_mtrl_bs_uom_cdv,
    blg_ln_mtrl_bs_uom_qty,
    blg_ln_mtrl_grss_wght_uom_qty,
    blg_ln_mtrl_net_wght_uom_qty,
    blg_ln_mtrl_qty,
    blg_ln_mtrl_uom_cdv,
    blg_ln_mtrl_vol_uom_cdv,
    blg_ln_mtrl_vol_uom_qty,
    blg_ln_mtrl_wght_uom_cdv,
    blg_ln_net_amt,
    blg_ln_net_tot_amt,
    blg_ln_net_tot_frgnccy_amt,
    blg_ln_net_uprc_amt,
    blg_ln_net_uprc_frgnccy_amt,
    blg_ln_num,
    blg_ln_rsn_cdv,
    blg_ln_rtnd_amt,
    blg_ln_shpmt_ctry_cdv,
    blg_ln_tax_pct,
    blg_ln_tot_dscnt_amt,
    blg_ln_tot_dscnt_frgnccy_amt,
    blg_ln_tot_prmtn_amt,
    blg_ln_tot_prmtn_frgnccy_amt,
    blg_ln_tot_tax_amt,
    blg_ln_tot_tax_frgnccy_amt,
    blg_ln_typ_cdv,
    blg_net_amt,
    blg_typ_cdv,
    bonba,
    btch_id,
    co_cdv,
    crncy_exch_rt_amt,
    crtd_by_id,
    crtd_dt,
    cust_ordr_crncy_cdv,
    cust_ordr_ln_num,
    cust_ordr_uniq_id_val,
 cust_po_typ_cdv,
    dlvry_doc_crt_dt,
    dlvry_doc_id,
    dlvry_ln_num,
    dstrbtn_chnl_cdv,
    dw_rownum,
    ean11,
    erzet_vbrp,
    fbuda,
    fkdat_ana,
    knuma,
    knumv_ana,
    kokrs,
    kondm,
    kostl,
    kursk,
    kursk_dat,
    kvgr5,
    kzwi1,
    kzwi2,
    kzwi3,
    kzwi4,
    kzwi5,
    kzwi6,
    land1,
    mandt,
    matkl,
    matwa,
    mtrl_id,
    mtrl_nm,
    mwsbp,
    ordr_crt_dt,
    ordr_crt_tm,
    posar,
    prctr,
    prodh,
    prsdt,
    ps_psp_pnr,
    reason_code,
    rfrnc_blg_ln_num,
    rte_id,
 sfakn,
    ship_to_cust_id,
    shkzg,
    sls_loc_id,
    sls_offc_loc_id_val,
    sls_org_cdv,
    sold_to_cust_id,
    spara,
    spart,
    stadat,
    upmat,
    uprc,
    vbak__auart,
 vbak__zz1_hh_no_sdh,
 vbrk__pre_fkart,
 vbrk__pre_vbeln,
    vgbel,
    vgpos,
 vgtyp,
    vkgrp,
    zdel_ind,
    zsysid,
    ztimestamp
  from ACQ_P_STAGE.pgt_sls_billing_item_stg
  group by 
    acq_mdm_sys_id,
    acq_pgt_sys_id,
 autyp,
    bill_to_cust_id,
    blg_cndtn_doc_id_val,
    blg_cndtn_typ_cdv,
    blg_crncy_cdv,
    blg_doc_dt,
    blg_doc_id,
    blg_ln_adjmt_amt,
    blg_ln_adjmt_frgnccy_amt,
    blg_ln_ctgy_cdv,
    blg_ln_grss_amt,
    blg_ln_grss_frgnccy_amt,
    blg_ln_grss_uprc_amt,
    blg_ln_grss_uprc_frgnccy_amt,
    blg_ln_mtrl_bs_uom_cdv,
    blg_ln_mtrl_bs_uom_qty,
    blg_ln_mtrl_grss_wght_uom_qty,
    blg_ln_mtrl_net_wght_uom_qty,
    blg_ln_mtrl_qty,
    blg_ln_mtrl_uom_cdv,
    blg_ln_mtrl_vol_uom_cdv,
    blg_ln_mtrl_vol_uom_qty,
    blg_ln_mtrl_wght_uom_cdv,
    blg_ln_net_amt,
    blg_ln_net_tot_amt,
    blg_ln_net_tot_frgnccy_amt,
    blg_ln_net_uprc_amt,
    blg_ln_net_uprc_frgnccy_amt,
    blg_ln_num,
    blg_ln_rsn_cdv,
    blg_ln_rtnd_amt,
    blg_ln_shpmt_ctry_cdv,
    blg_ln_tax_pct,
    blg_ln_tot_dscnt_amt,
    blg_ln_tot_dscnt_frgnccy_amt,
    blg_ln_tot_prmtn_amt,
    blg_ln_tot_prmtn_frgnccy_amt,
    blg_ln_tot_tax_amt,
    blg_ln_tot_tax_frgnccy_amt,
    blg_ln_typ_cdv,
    blg_net_amt,
    blg_typ_cdv,
    bonba,
    btch_id,
    co_cdv,
    crncy_exch_rt_amt,
    crtd_by_id,
    crtd_dt,
    cust_ordr_crncy_cdv,
    cust_ordr_ln_num,
    cust_ordr_uniq_id_val,
 cust_po_typ_cdv,
    dlvry_doc_crt_dt,
    dlvry_doc_id,
    dlvry_ln_num,
    dstrbtn_chnl_cdv,
    dw_rownum,
    ean11,
    erzet_vbrp,
    fbuda,
    fkdat_ana,
    knuma,
    knumv_ana,
    kokrs,
    kondm,
    kostl,
    kursk,
    kursk_dat,
    kvgr5,
    kzwi1,
    kzwi2,
    kzwi3,
    kzwi4,
    kzwi5,
    kzwi6,
    land1,
    mandt,
    matkl,
    matwa,
    mtrl_id,
    mtrl_nm,
    mwsbp,
    ordr_crt_dt,
    ordr_crt_tm,
    posar,
    prctr,
    prodh,
    prsdt,
    ps_psp_pnr,
    reason_code,
    rfrnc_blg_ln_num,
    rte_id,
 sfakn,
    ship_to_cust_id,
    shkzg,
    sls_loc_id,
    sls_offc_loc_id_val,
    sls_org_cdv,
    sold_to_cust_id,
    spara,
    spart,
    stadat,
    upmat,
    uprc,
    vbak__auart,
 vbak__zz1_hh_no_sdh,
 vbrk__pre_fkart,
 vbrk__pre_vbeln,
    vgbel,
    vgpos,
 vgtyp,
    vkgrp,
    zdel_ind,
    zsysid,
    ztimestamp
  having count(*) > 1
  ) tbl;


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
    'RCD_PK_DUPLICATE',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    'all',
    'all',
    'CRIT',
    'pgt_sls_billing_item_stg',
    null
  from 
  (select 
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid
  from 
  (select
    acq_mdm_sys_id,
    acq_pgt_sys_id,
 autyp,
    bill_to_cust_id,
    blg_cndtn_doc_id_val,
    blg_cndtn_typ_cdv,
    blg_crncy_cdv,
    blg_doc_dt,
    blg_doc_id,
    blg_ln_adjmt_amt,
    blg_ln_adjmt_frgnccy_amt,
    blg_ln_ctgy_cdv,
    blg_ln_grss_amt,
    blg_ln_grss_frgnccy_amt,
    blg_ln_grss_uprc_amt,
    blg_ln_grss_uprc_frgnccy_amt,
    blg_ln_mtrl_bs_uom_cdv,
    blg_ln_mtrl_bs_uom_qty,
    blg_ln_mtrl_grss_wght_uom_qty,
    blg_ln_mtrl_net_wght_uom_qty,
    blg_ln_mtrl_qty,
    blg_ln_mtrl_uom_cdv,
    blg_ln_mtrl_vol_uom_cdv,
    blg_ln_mtrl_vol_uom_qty,
    blg_ln_mtrl_wght_uom_cdv,
    blg_ln_net_amt,
    blg_ln_net_tot_amt,
    blg_ln_net_tot_frgnccy_amt,
    blg_ln_net_uprc_amt,
    blg_ln_net_uprc_frgnccy_amt,
    blg_ln_num,
    blg_ln_rsn_cdv,
    blg_ln_rtnd_amt,
    blg_ln_shpmt_ctry_cdv,
    blg_ln_tax_pct,
    blg_ln_tot_dscnt_amt,
    blg_ln_tot_dscnt_frgnccy_amt,
    blg_ln_tot_prmtn_amt,
    blg_ln_tot_prmtn_frgnccy_amt,
    blg_ln_tot_tax_amt,
    blg_ln_tot_tax_frgnccy_amt,
    blg_ln_typ_cdv,
    blg_net_amt,
    blg_typ_cdv,
    bonba,
    btch_id,
    co_cdv,
    crncy_exch_rt_amt,
    crtd_by_id,
    crtd_dt,
    cust_ordr_crncy_cdv,
    cust_ordr_ln_num,
    cust_ordr_uniq_id_val,
 cust_po_typ_cdv,
    dlvry_doc_crt_dt,
    dlvry_doc_id,
    dlvry_ln_num,
    dstrbtn_chnl_cdv,
    dw_rownum,
    ean11,
    erzet_vbrp,
    fbuda,
    fkdat_ana,
    knuma,
    knumv_ana,
    kokrs,
    kondm,
    kostl,
    kursk,
    kursk_dat,
    kvgr5,
    kzwi1,
    kzwi2,
    kzwi3,
    kzwi4,
    kzwi5,
    kzwi6,
    land1,
    mandt,
    matkl,
    matwa,
    mtrl_id,
    mtrl_nm,
    mwsbp,
    ordr_crt_dt,
    ordr_crt_tm,
    posar,
    prctr,
    prodh,
    prsdt,
    ps_psp_pnr,
    reason_code,
    rfrnc_blg_ln_num,
    rte_id,
 sfakn,
    ship_to_cust_id,
    shkzg,
    sls_loc_id,
    sls_offc_loc_id_val,
    sls_org_cdv,
    sold_to_cust_id,
    spara,
    spart,
    stadat,
    upmat,
    uprc,
    vbak__auart,
 vbak__zz1_hh_no_sdh,
 vbrk__pre_fkart,
 vbrk__pre_vbeln,
    vgbel,
    vgpos,
 vgtyp,
    vkgrp,
    zdel_ind,
    zsysid,
    ztimestamp
  from ACQ_P_STAGE.pgt_sls_billing_item_stg
  group by
    acq_mdm_sys_id,
    acq_pgt_sys_id,
 autyp,
    bill_to_cust_id,
    blg_cndtn_doc_id_val,
    blg_cndtn_typ_cdv,
    blg_crncy_cdv,
    blg_doc_dt,
    blg_doc_id,
    blg_ln_adjmt_amt,
    blg_ln_adjmt_frgnccy_amt,
    blg_ln_ctgy_cdv,
    blg_ln_grss_amt,
    blg_ln_grss_frgnccy_amt,
    blg_ln_grss_uprc_amt,
    blg_ln_grss_uprc_frgnccy_amt,
    blg_ln_mtrl_bs_uom_cdv,
    blg_ln_mtrl_bs_uom_qty,
    blg_ln_mtrl_grss_wght_uom_qty,
    blg_ln_mtrl_net_wght_uom_qty,
    blg_ln_mtrl_qty,
    blg_ln_mtrl_uom_cdv,
    blg_ln_mtrl_vol_uom_cdv,
    blg_ln_mtrl_vol_uom_qty,
    blg_ln_mtrl_wght_uom_cdv,
    blg_ln_net_amt,
    blg_ln_net_tot_amt,
    blg_ln_net_tot_frgnccy_amt,
    blg_ln_net_uprc_amt,
    blg_ln_net_uprc_frgnccy_amt,
    blg_ln_num,
    blg_ln_rsn_cdv,
    blg_ln_rtnd_amt,
    blg_ln_shpmt_ctry_cdv,
    blg_ln_tax_pct,
    blg_ln_tot_dscnt_amt,
    blg_ln_tot_dscnt_frgnccy_amt,
    blg_ln_tot_prmtn_amt,
    blg_ln_tot_prmtn_frgnccy_amt,
    blg_ln_tot_tax_amt,
    blg_ln_tot_tax_frgnccy_amt,
    blg_ln_typ_cdv,
    blg_net_amt,
    blg_typ_cdv,
    bonba,
    btch_id,
    co_cdv,
    crncy_exch_rt_amt,
    crtd_by_id,
    crtd_dt,
    cust_ordr_crncy_cdv,
    cust_ordr_ln_num,
    cust_ordr_uniq_id_val,
 cust_po_typ_cdv,
    dlvry_doc_crt_dt,
    dlvry_doc_id,
    dlvry_ln_num,
    dstrbtn_chnl_cdv,
    dw_rownum,
    ean11,
    erzet_vbrp,
    fbuda,
    fkdat_ana,
    knuma,
    knumv_ana,
    kokrs,
    kondm,
    kostl,
    kursk,
    kursk_dat,
    kvgr5,
    kzwi1,
    kzwi2,
    kzwi3,
    kzwi4,
    kzwi5,
    kzwi6,
    land1,
    mandt,
    matkl,
    matwa,
    mtrl_id,
    mtrl_nm,
    mwsbp,
    ordr_crt_dt,
    ordr_crt_tm,
    posar,
    prctr,
    prodh,
    prsdt,
    ps_psp_pnr,
    reason_code,
    rfrnc_blg_ln_num,
    rte_id,
 sfakn,
    ship_to_cust_id,
    shkzg,
    sls_loc_id,
    sls_offc_loc_id_val,
    sls_org_cdv,
    sold_to_cust_id,
    spara,
    spart,
    stadat,
    upmat,
    uprc,
    vbak__auart,
 vbak__zz1_hh_no_sdh,
 vbrk__pre_fkart,
 vbrk__pre_vbeln,
    vgbel,
    vgpos,
 vgtyp,
    vkgrp,
    zdel_ind,
    zsysid,
    ztimestamp) tbl2
  group by 
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid
  having count(*) > 1
  ) tbl;

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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: bill_to_cust_id, acq_pgt_sys_id, mandt',
    'VALS: ' || tbl.fknk_bill_to_cust_id || '|' || tbl.fknk_acq_pgt_sys_id || '|' || tbl.fknk_mandt
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_BILL_CK as tbl
  where fk_BILL_CK_surr is null;


  update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_BILL_CK
  from ACQ_P_DIM.gtmd_ddh_cust_core_cf_key
    set fk_BILL_CK_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_item_stg_BILL_CK.fknk_bill_to_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_item_stg_BILL_CK.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id and
    fk_pgt_sls_billing_item_stg_BILL_CK.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_BILL_CK.fk_BILL_CK_surr is null;

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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: mandt, acq_pgt_sys_id, mtrl_id',
    'VALS: ' || tbl.fknk_mandt || '|' || tbl.fknk_acq_pgt_sys_id || '|' || tbl.fknk_mtrl_id
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_IK as tbl
  where fk_IK_surr is null;

update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_IK
  from ACQ_P.gtmd_ddh_mtrl_core_cf_key
    set fk_IK_surr = gtmd_ddh_mtrl_core_cf_key.dw_item_id
  where
    fk_pgt_sls_billing_item_stg_IK.fknk_mandt = gtmd_ddh_mtrl_core_cf_key.client_id and
    fk_pgt_sls_billing_item_stg_IK.fknk_acq_pgt_sys_id = gtmd_ddh_mtrl_core_cf_key.pgt_sys_id and
    fk_pgt_sls_billing_item_stg_IK.fknk_mtrl_id = gtmd_ddh_mtrl_core_cf_key.mtrl_id
    and gtmd_ddh_mtrl_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_IK.fk_IK_surr is null;


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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: acq_pgt_sys_id, sls_loc_id, mandt',
    'VALS: ' || tbl.fknk_acq_pgt_sys_id || '|' || tbl.fknk_sls_loc_id || '|' || tbl.fknk_mandt
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_LK as tbl
  where fk_LK_surr is null;


   update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_LK
  from ACQ_P_DIM.gtmd_ddh_loc_core_cf_key
    set fk_LK_surr = gtmd_ddh_loc_core_cf_key.dw_loc_id
  where
    fk_pgt_sls_billing_item_stg_LK.fknk_acq_pgt_sys_id = gtmd_ddh_loc_core_cf_key.pgt_sys_id and
    fk_pgt_sls_billing_item_stg_LK.fknk_sls_loc_id = gtmd_ddh_loc_core_cf_key.loc_id and
    fk_pgt_sls_billing_item_stg_LK.fknk_mandt = gtmd_ddh_loc_core_cf_key.client_id
    and gtmd_ddh_loc_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_LK.fk_LK_surr is null;



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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: rte_id, mandt, acq_pgt_sys_id',
    'VALS: ' || tbl.fknk_rte_id || '|' || tbl.fknk_mandt || '|' || tbl.fknk_acq_pgt_sys_id
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_RK as tbl
  where fk_RK_surr is null;


  update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_RK
  from ACQ_P.gtmd_ddh_rte_core_cf_key
    set fk_RK_surr = gtmd_ddh_rte_core_cf_key.dw_rte_id
  where
    fk_pgt_sls_billing_item_stg_RK.fknk_rte_id = gtmd_ddh_rte_core_cf_key.rte_id and
    fk_pgt_sls_billing_item_stg_RK.fknk_mandt = gtmd_ddh_rte_core_cf_key.client_id and
    fk_pgt_sls_billing_item_stg_RK.fknk_acq_pgt_sys_id = gtmd_ddh_rte_core_cf_key.pgt_sys_id
    and gtmd_ddh_rte_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_RK.fk_RK_surr is null;



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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: ship_to_cust_id, mandt, acq_pgt_sys_id',
    'VALS: ' || tbl.fknk_ship_to_cust_id || '|' || tbl.fknk_mandt || '|' || tbl.fknk_acq_pgt_sys_id
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_SHIP_CK as tbl
  where fk_SHIP_CK_surr is null;


  update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_SHIP_CK
  from ACQ_P_DIM.gtmd_ddh_cust_core_cf_key
    set fk_SHIP_CK_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_item_stg_SHIP_CK.fknk_ship_to_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_item_stg_SHIP_CK.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id and
    fk_pgt_sls_billing_item_stg_SHIP_CK.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_SHIP_CK.fk_SHIP_CK_surr is null;


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
    blg_doc_id,
    blg_ln_num,
    mandt,
    zsysid, 
    'RCD_FK_NO_REF',
    cast('blg_doc_id|blg_ln_num|mandt|zsysid' as VARCHAR(200)),
    cast(coalesce(cast(tbl.blg_doc_id as VARCHAR(40)), 'NULL') || '|' || coalesce(cast(tbl.blg_ln_num as VARCHAR(24)), 'NULL') || '|' || coalesce(cast(tbl.mandt as VARCHAR(12)), 'NULL') || '|' || coalesce(cast(tbl.zsysid as VARCHAR(32)), 'NULL') as VARC
HAR(1000)),
    null,
    null,
    'ERR',
    'COLS: sold_to_cust_id, acq_pgt_sys_id, mandt',
    'VALS: ' || tbl.fknk_sold_to_cust_id || '|' || tbl.fknk_acq_pgt_sys_id || '|' || tbl.fknk_mandt
  from ACQ_P_WORK.fk_pgt_sls_billing_item_stg_SOLD_CK as tbl
  where fk_SOLD_CK_surr is null;


  update ACQ_P_WORK.fk_pgt_sls_billing_item_stg_SOLD_CK
  from ACQ_P_DIM.gtmd_ddh_cust_core_cf_key
    set fk_SOLD_CK_surr = gtmd_ddh_cust_core_cf_key.dw_cust_id
  where
    fk_pgt_sls_billing_item_stg_SOLD_CK.fknk_sold_to_cust_id = gtmd_ddh_cust_core_cf_key.cust_id and
    fk_pgt_sls_billing_item_stg_SOLD_CK.fknk_acq_pgt_sys_id = gtmd_ddh_cust_core_cf_key.pgt_sys_id and
    fk_pgt_sls_billing_item_stg_SOLD_CK.fknk_mandt = gtmd_ddh_cust_core_cf_key.client_id
    and gtmd_ddh_cust_core_cf_key.prmy_ind = 'Y'
    and fk_pgt_sls_billing_item_stg_SOLD_CK.fk_SOLD_CK_surr is null;


  select
    err_cd || '|' ||
    err_svrty || '|' ||
    cast(total as varchar(100))  (TITLE '')
  from
  (select 
    err_cd, 
    err_svrty, 
    count(*) as total
  from MYDAT_ERR_EVNT
  group by 1,2 
  UNION ALL
  select 
    'ALL',
    err_svrty, 
    count(*) as total
  from MYDAT_ERR_EVNT
  group by 2) tbl;


   SELECT
                '<ExceptionRequest xmlns="http://www.PepsiCo.com/unique/default/namespace/CommonLE">
                <Header>
                <ApplicationID>ACQ</ApplicationID>
                <ServiceName>ACQ_LOADER</ServiceName>
                <ComponentName>SOURCE_TO_CORE</ComponentName>
                <Hostname>peplap00726</Hostname>
                <Timestamp>2025-08-07T11:33:40</Timestamp>
                </Header>
                <Category>LOAD_ERROR</Category>
                <Type>'||UPPER('pgt')||'</Type>
                <Severity>2</Severity>
                <Code>N/A</Code>
                <Message>Error Records: '||TRIM(ERR_COUNT)||'; Error table: pgt_sls_billing_item_err</Message>
                <DumpAnalysis>&#xD;----------------- &#xD; SYSTEM: ACQ;  ACTIVITY: PGT_SLS_BILLING_ITEM;  STEP: PGT_SLS_BILLING_ITEM_LOAD;  TABLE: pgt_sls_billing_item_err;&#xD;STEP_ID: '||DW_STEP_ID||';  BATCH_ID: '||DW_BTCH_ID||';&#xD;QUERY: select * from ACQ_P_JOB.pgt_sls_billing_item_err where DW_BTCH_ID in ('||DW_BTCH_ID||');&#xD;Web Link: https://ews.mypepsico.com/acq_err/?acq_table=pgt_sls_billing_item_err&amp;acq_max=100&amp;acq_date=&amp;action=View&amp;mode=rejected&amp;acq_batch='||DW_BTCH_ID||'</DumpAnalysis>
                </ExceptionRequest>' (TITLE '')
        FROM
                (       SELECT
                                TRIM(DW_BTCH_ID(INTEGER)) AS DW_BTCH_ID, 
        TRIM(DW_STEP_ID(INTEGER)) AS DW_STEP_ID,
        COUNT(*) as ERR_COUNT
                        FROM
                                ACQ_P_JOB.pgt_sls_billing_item_err ERR
                                JOIN PEPCMN_P.ACTVTY A
                                        ON ERR.DW_BTCH_ID = A.CUR_BTCH_ID
                                JOIN PEPCMN_P.STEP S
                                        ON A.ACTVTY_ID=S.ACTVTY_ID
                                        AND ERR.DW_STEP_ID=S.STEP_ID
                        WHERE
                                SYS_NM='ACQ'
                                AND ACTVTY_NM='PGT_SLS_BILLING_ITEM'
                        GROUP BY 1,2
                        HAVING ERR_COUNT>0
                )A;

SELECT
                '<ExceptionRequest xmlns="http://www.PepsiCo.com/unique/default/namespace/CommonLE">
                <Header>
                <ApplicationID>ACQ</ApplicationID>
                <ServiceName>ACQ_LOADER</ServiceName>
                <ComponentName>SOURCE_TO_CORE</ComponentName>
                <Hostname>peplap00726</Hostname>
                <Timestamp>2025-08-07T11:33:40</Timestamp>
                </Header>
                <Category>LOAD_ERROR</Category>
                <Type>'||UPPER('pgt')||'</Type>
                <Severity>2</Severity>
                <Code>N/A</Code>
                <Message>Error Records: '||TRIM(ERR_COUNT)||'; Error table: pgt_sls_billing_item_err2</Message>
				<DumpAnalysis>&#xD;-----------------&#xD;SYSTEM: ACQ;  ACTIVITY: PGT_SLS_BILLING_ITEM;  STEP: PGT_SLS_BILLING_ITEM_LOAD;  TABLE: pgt_sls_billing_item_err;&#xD;STEP_ID: '||DW_STEP_ID||';  BATCH_ID: '||DW_BTCH_ID||';&#xD;QUERY: select * from ACQ_P_JOB.pgt_sls_billing_item_err2 where DW_BTCH_ID in ('||DW_BTCH_ID||');&#xD;Web Link: https://ews.mypepsico.com/acq_err/?acq_table=pgt_sls_billing_item_err2&amp;acq_max=100&amp;acq_date=&amp;action=View&amp;mode=rejected&amp;acq_batch='||DW_BTCH_ID||'</DumpAnalysis>
                </ExceptionRequest>' (TITLE '')
        FROM
                (       SELECT
                                TRIM(DW_BTCH_ID(INTEGER)) AS DW_BTCH_ID, 
        TRIM(DW_STEP_ID(INTEGER)) AS DW_STEP_ID,
        COUNT(*) as ERR_COUNT
                        FROM
                                ACQ_P_JOB.pgt_sls_billing_item_err2 ERR
                                JOIN PEPCMN_P.ACTVTY A
                                        ON ERR.DW_BTCH_ID = A.CUR_BTCH_ID
                                JOIN PEPCMN_P.STEP S
                                        ON A.ACTVTY_ID=S.ACTVTY_ID
                                        AND ERR.DW_STEP_ID=S.STEP_ID
                        WHERE
                                SYS_NM='ACQ'
                                AND ACTVTY_NM='PGT_SLS_BILLING_ITEM'
                        GROUP BY 1,2
                        HAVING ERR_COUNT>0
                )A;

