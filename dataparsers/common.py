from libnntsc.dberrorcodes import *

def create_new_stream(db, exp, basecol, subtype, streamcols, result,
        timestamp, streamtable, datatable):

    streamprops = {}
    for col in streamcols:
        if col['name'] in result:
            streamprops[col['name']] = result[col['name']]
        else:
            streamprops[col['name']] = None

    while 1:
        errorcode = DB_NO_ERROR
        colid, streamid = db.insert_stream(streamtable, datatable,
            timestamp, streamprops)

        if colid < 0:
            errorcode = streamid

        if streamid < 0:
            errorcode = streamid
    
        if errorcode == DB_QUERY_TIMEOUT:
            continue
        if errorcode != DB_NO_ERROR:
            return errorcode
    
        err = db.commit_streams()
        if err == DB_QUERY_TIMEOUT:
            continue
        if err != DB_NO_ERROR:
            return err
        break

    if exp == None:
        return streamid

    err, colid = db.get_collection_id(basecol, subtype)
    if err != DB_NO_ERROR:
        return err
    if colid == 0:
        return streamid

    exp.publishStream(colid, basecol + "_" + subtype, streamid, streamprops)
    return streamid

def insert_data(db, exp, stream, ts, result, datacols, colname, datatable,
        casts = {}):
    filtered = {}
    for col in datacols:
        if col["name"] in result:
            filtered[col["name"]] = result[col["name"]]
        else:
            filtered[col["name"]] = None

    err = db.insert_data(datatable, colname, stream, ts, filtered, casts)
    if err != DB_NO_ERROR:
        return err

    if exp != None:
        exp.publishLiveData(colname, stream, ts, filtered)

    return DB_NO_ERROR



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
