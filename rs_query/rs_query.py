
# -*- coding: utf-8 -*-
"""
#### rs_query v1.01 Beta #####
Last Update:  Tuesday, January 05  2016
@author: Datuman a.k.a. kod5kod
https://github.com/kod5kod

The "rs_query" module is a simple "psycopg2" wrapper for quering and interacting with Amazon's Redshift.

Dependencies:
import time,math,re
import psycopg2

Inputs:
RS_Query(redshift_conf, s3_conf = False)
redshift_conf = a dictionary with the following keys: host, dbname, port,user, password. 
Example of redshit_conf:
redshift_conf = {
'host' : 'your_host_address.redshift.amazonaws.com',
'dbname' : 'your_dbname',
'port' : '1234',
'user' : 'user_name',
'password' : 'user_password'
}

Outpot:
See methods. 

Status:
print(RS_Query_instance)
"""
import time,math
import psycopg2


class RS_Query:
    """This wrapper is based on psycopg2.
    The "rs_query" module is a simple "psycopg2" wrapper for quering and interacting with Amazon's Redshift.
	#	Dependencies:
	import time,math,re
	import psycopg2
	#	Inputs:
	RS_Query(redshift_conf, s3_conf = False)
	redshift_conf = a dictionary with the following keys: host, dbname, port,user, password. 
	#	Outpot:
	See methods. 
	#	Status:
	print(RS_Query_instance)
    """

    def __init__(self, redshift_conf, s3_conf = False):
        # Initializing the configs:
        self.redshift_conf = redshift_conf
        self.s3_conf = s3_conf
        self.host = redshift_conf['host']
        self.dbname = redshift_conf['dbname']
        self.port = redshift_conf['port']
        self.user = redshift_conf['user']
        self.password = redshift_conf['password']
        self.conn_string = 'host={0} dbname={1} port={2}  user={3} password={4}'.format(self.host, self.dbname,self.port,self.user,self.password)
        if s3_conf:
            self.aws_access_key_id = s3_conf['aws_access_key_id']
            self.aws_secret_access_key = s3_conf['aws_secret_access_key']
        else:
            self.aws_access_key_id = ''
            self.aws_secret_access_key = ''
        # Openning a connection:
        self.conn = psycopg2.connect(self.conn_string)
        self.conn_starttime = time.asctime( time.localtime(time.time()) )
        print 'Connection  established at: ' + self.conn_starttime
        self.open_connection = True
        self.conn_endtime = ''

    def __str__(self):
        stars = '\n***************************\n' 
        sen1 = 'Redshift Config Info : \n {}'.format(self.redshift_conf)
        sen2 = 'Connection String Is : \n {}'.format(self.conn_string)
        sen3 = 'S3 Config Info : \n {}'.format(self.s3_conf)
        if self.open_connection:
            sen4 = 'Connection status: Active!'
        else:
            sen4 = 'Connection status: Inactive!'
        sen5 = 'Connection initiated (if any) at: ' + self.conn_starttime
        sen6 = 'Connection ended (if any) at: ' + self.conn_endtime

        # printout:
        return stars + sen1 + '\n\n' + sen2 + '\n\n'+sen3 + '\n\n'+sen4 + '\n'+sen5 + '\n' +sen6 + '\n'+stars
        

    def send_query(self,query):
        tic = time.time() # measure retrieval time
        #  Creating a cursor:
        cursor = self.conn.cursor()
        cursor.execute(query)
        # Saving the data queried in the variable "records":
        records = cursor.fetchall()
        cursor.close()
        # Getting header information:
        header = []
        for column_name in cursor.description:
            header.append(column_name[0])
        toc = time.time() # end measuring time
        QTime = (toc-tic)/60
        minutes = int(QTime)
        seconds = int(round(60*((QTime -math.floor(QTime)))))
        print "Redshift query succesful. Quering took {} minutes and {} seconds \n".format(minutes,seconds) 
        return(records,header)

    def send_into(self,into_query):
        tic = time.time() # measure retrieval time
        #  Creating a cursor:
        cursor = self.conn.cursor()
        cursor.execute(into_query)
        cursor.close()
        toc = time.time() # end measuring time
        QTime = (toc-tic)/60
        minutes = int(QTime)
        seconds = int(round(60*((QTime -math.floor(QTime)))))
        print "Redshift into query succesful. Quering took {} minutes and {} seconds \n".format(minutes,seconds)


    def send_unload(self,unload_query,s3_path='',auto = False):
        import re
        #  Input: a non-unloadable query (string) Output: an unloadable-ready query with unload command appended and "'" escaped (string)
        localtime = time.asctime( time.localtime(time.time()) )
        if auto and s3_path:
            unloadable_query = re.sub('\'', '\\\'', unload_query)
            query = """unload('{0}')
                            to  '{1}' credentials 'aws_access_key_id={2};aws_secret_access_key={3}'
                            parallel off allowoverwrite delimiter ',';""".format(unloadable_query,s3_path,self.aws_access_key_id, self.aws_secret_access_key)
            print '\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n Local current time : ' +localtime +'\n Running the following unload query: \n' +query + '\n\n Quering in progress, do not terminate action.'
        else:
            query = unload_query
        tic = time.time() # measure retrieval time
        #  Creating a cursor:
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()
        toc = time.time() # end measuring time
        QTime = (toc-tic)/60
        minutes = int(QTime)
        seconds = int(round(60*((QTime -math.floor(QTime)))))
        print "Redshift unload query succesful. Quering took {} minutes and {} seconds \n".format(minutes,seconds)
		
		
    def commit(self):
        # Make the changes to the database persistent
        conn.commit()
        print "commiting changes to the database (persist)"
		
		
    def rollback(self):
        # Rolling back to last commit
        conn.rollback()
        print "Rolling back to last commit"
	
	
    def disconnect(self):
        #self.cursor.close()
        self.conn.close()
        self.open_connection = False
        self.conn_endtime = time.asctime( time.localtime(time.time()) )
        print  'Connection  ended at: ' + self.conn_endtime


