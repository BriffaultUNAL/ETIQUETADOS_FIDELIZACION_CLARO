SELECT
    call_date,
    phone_number_dialed,
    campaign_id,
    status,
    user,
    list_id,
    length_in_sec,
    lead_id,
    uniqueid,
    caller_code,
    IP_DESCARGA
FROM
    bbdd_groupcos_repositorio_planta_telefonica.`tb_marcaciones_vicidial_out_172.70.7.56`
where
    date(call_date) = date(date_sub(now(), interval :interval day))
    and campaign_id in ('FIDICS')
    and list_id != 998;