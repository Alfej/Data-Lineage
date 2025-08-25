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
