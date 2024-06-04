SELECT
    start_time,
    filename,
    location,
    lead_id as lead_id1
FROM
    recording_log
WHERE
    date(start_time) = date(date_sub(now(), interval :interval day));