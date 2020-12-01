import pyodbc
import psycopg2
import lithops

def connect(tablename):

    #Fixed conexion string for connecting sqlserver -- no need to change 
    conn1 = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=cap-au-sg-prd-04.securegateway.appdomain.cloud,15275;'
        'DATABASE=jtiiasset;'
        'UID=sa;'
        'PWD=Pas5word')

    #Fixed conexion string for connecting postgresql -- no need to change  
    conn2 = psycopg2.connect(
        database = 'ibmclouddb' ,
        user = 'ibm_cloud_f261f536_a6f2_4fec_b8e9_55016c16b459' ,
        password = 'a4285df0d0f18926f9c84591c78f91d402ab3d037e8ef6023f0fb4ff41e45043',
        host = 'c0ca2771-62ed-4c2a-862e-743fda10b364.bqfh4fpt0vhjh7rs4ot0.databases.appdomain.cloud',
        port = '32645')
    
    #Retrieve data -- change here
    cur1 = conn1.cursor()
    cur1.execute("SELECT id, stat, createdby, createddate, createdip, updatedby, updateddate, updatedip, date_deploy, date_undeploy, asset_id, city_deploy_id, city_undeploy_id, employee_samaccountname FROM "+ tablename)
    records = cur1.fetchall()
    #conn1.commit() -- no need to commit

    #Delete data --change here
    cur2 = conn2.cursor()
    cur2.execute("DELETE FROM jtiiasset." +tablename)
    conn2.commit()

    print(cur2.rowcount, "Records deleted successfully from " +tablename)

    #Insert data -- change here
    cur2 = conn2.cursor()
    cur2.executemany("INSERT INTO jtiiasset." +tablename+ "(id, stat, createdby, createddate, createdip, updatedby, updateddate, updatedip, date_deploy, date_undeploy, asset_id, city_deploy_id, city_undeploy_id, employee_samaccountname) \
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",records)
    conn2.commit()

    print(cur2.rowcount, "Record inserted successfully into " +tablename)


if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    fexec.call_async(connect,'asset_inventory_deployment')
    print(fexec.get_result())