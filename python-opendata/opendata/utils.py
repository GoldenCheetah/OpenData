import arrow


def datestring_to_activity_id(datestring):
    dt = arrow.get(datestring, 'YYYY/MM/DD HH:mm:ss')
    return dt.format('YYYY_MM_DD_HH_mm_ss')
