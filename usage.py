import json
import time
import pymysql
from rgwadmin import RGWAdmin

# radosgw-admin user create --uid={username} --display-name="{display-name}"
# radosgw-admin caps add --uid={uid} --caps="users=read;buckets=read;metadata=read;usage=read;zone=read"


def updateBucketInfo():
    conn = pymysql.connect("localhost", "root", "PASSWORD", "rgw_info")
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS user_info(user_id varchar(100) not null primary key ,"
                   "display_name varchar(100) ,max_buckets int ,bucketlist longtext not null,max_size_kb bigint ,"
                   "max_objects int ,user_quota_enable varchar(100) ,user_bucket_quota_enable varchar (100)) ")
    cursor.execute("CREATE TABLE IF NOT EXISTS bucket_info(bucket_id varchar(100) not null primary key ,"
                   " usaged varchar(100) ,usaged_gb int,"
                   "num_objects int ,bucket_max_size_gb int ,max_objects int ,bucket_quota_enable varchar(100) )")
    rgw = RGWAdmin(
        access_key='*******************',
        secret_key='*************************',
        server='**.**.**.**:7480',
        verify=False, secure=False)

    userlist = rgw.get_users()

    for i in range(len(userlist)):
        userinfo = rgw.get_user(uid=userlist[i])
        bucketlist = rgw.get_bucket(uid=userlist[i])
        userquota = rgw.get_user_quota(uid=userlist[i])
        userbucketquota = rgw.get_user_bucket_quota(uid=userlist[i])
        bucketlist_str = ",".join(bucketlist)
        print(bucketlist_str)
        user_sql = "replace into user_info values ('%s','%s','%s','%s','%s','%s','%s','%s') " \
                   % (userinfo['user_id'],
                      userinfo['display_name'],
                      userinfo['max_buckets'],
                      bucketlist_str,
                      userquota['max_size_kb'],
                      userquota['max_objects'],
                      userquota['enabled'],
                      userbucketquota['enabled'])
        cursor.execute(user_sql)
        conn.commit()
        print("userinfo:", userinfo)
        print("bucketlist:", bucketlist)
        for j in range(len(bucketlist)):
            bucketinfo = rgw.get_bucket(bucket=bucketlist[j])
            bucketquota = bucketinfo['bucket_quota']
            try:
                bucket_rgw = bucketinfo['usage']['rgw.main']
                print(bucket_rgw)
            except:
                bucket_rgw = {'size_kb_actual': 0, 'num_objects': 0}
            bucket_sql = "replace into bucket_info values ('%s','%s','%s','%s','%s','%s','%s') " \
                         % (bucketinfo['bucket'],
                            bucket_rgw['size_kb_actual'] / bucketquota['max_size_kb'],
                            bucket_rgw['size_kb_actual'] / 1024 / 1024,
                            bucket_rgw['num_objects'],
                            bucketquota['max_size_kb'] / 1024 / 1024,
                            bucketquota['max_objects'],
                            bucketquota['enabled'])
            cursor.execute(bucket_sql)
            conn.commit()

    conn.close()


def getUserInfo (uid, secrectkey):
    rgw = RGWAdmin(
        access_key='*******************************',
        secret_key='*******************************',
        server='**.**.**.**:7480',
        verify=False, secure=False)
    sub_secrect=rgw.get_user(uid=uid)['keys']['secret_key']
    if secrectkey == sub_secrect:
        conn = pymysql.connect("localhost", "root", "password", "rgw_info")
        cursor = conn.cursor()
        search_sql = "select * from user_info  where user_id = '%(uid)s'"
        cursor.execute(search_sql)
        search_result = cursor.fetchmany()
        print(search_result)
        conn.close()
        jsonData = []
        for row in search_result:
            result = {'user_id': row[0], 'display_name': row[1], 'max_buckets': row[2], 'bucketlist': row[3],
                      'max_size_kb': row[4], 'max_objects ': row[5], 'user_quota_enable': row[6],
                      'user_bucket_quota_enable': row[7]}
            jsonData.append(result)
    return result


def getBucketInfo(uid, secrectkey):
    rgw = RGWAdmin(
        access_key='*******************************',
        secret_key='*******************************',
        server='**.**.**.**:7480',
        verify=False, secure=False)
    sub_secrect = rgw.get_user(uid=uid)['keys']['secret_key']
    if secrectkey == sub_secrect:
        conn = pymysql.connect("localhost", "root", "PASSWORD", "rgw_info")
        cursor = conn.cursor()
        search_sql = "select * from user_info  where bucket_id = '%(bucket_id)s'"
        cursor.execute(search_sql)
        search_result = cursor.fetchmany()
        print(search_result)
        conn.close()
        jsonData = []
        for row in search_result:
            result = {'bucket_id': row[0], 'usaged': row[1], 'usaged_gb': row[2], 'num_objects': row[3],
                      'bucket_max_size_gb': row[4], 'max_objects ': row[5], 'bucket_quota_enable': row[6]}
            jsonData.append(result)
    return result
