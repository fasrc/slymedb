'''
Created on May 19, 2014

@author: aaronkitzmiller
'''
import unittest
import os, re
from slyme import Slurm, JobReport
from slymedb import Store

"""
Job report text is pipe separated values of the following fields:

JobID|User|JobName|State|Partition|NCPUS|NNodes|CPUTime|TotalCPU|UserCPU|SystemCPU|ReqMem|MaxRSS|Start|End|NodeList|Elapsed

"""

class FakeRunSh:
    """
    Imitation runsh_i that uses predefined text
    """
    def __init__(self,text):
        self.text = text
        
    def runsh_i(self,args=[]):
        """
        Splits the text into lines and yields each line as though it were a 
        command output
        """       
        lines = self.text.splitlines()
        for line in lines:
            yield line


class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass
    
    def testReplaceJob(self):
        """
        If a job id is seen a second time, it should replace the existing one
        """
        jobid = "10048462"
        oldcputime = "02:08:33"
        newcputime = "03:00:00"
        text="\
%s|akitzmiller|bash|COMPLETED|interact|1|1|%s|08:01.433|06:47.955|01:13.477|2000Mn|2409232K|2014-05-01T11:43:26|2014-05-01T13:51:59|holy2a18206|00:00:10\
        " % (jobid,oldcputime)
        replacement="\
%s|akitzmiller|bash|COMPLETED|interact|1|1|%s|08:01.433|06:47.955|01:13.477|2000Mn|2409232K|2014-05-01T11:43:26|2014-05-01T13:51:59|holy2a18206|00:00:10\
        " % (jobid,newcputime)
        text = re.sub(r'\s+', '', text)
        replacement = re.sub(r'\s+','', replacement)
        
        currentrunsh = FakeRunSh(text)
        jobreports = Slurm.getJobReports(execfunc = currentrunsh.runsh_i)
        
        connectstring = os.environ.get('SLYMEDB_TEST_CONNECT_STRING')
        if not connectstring:
            raise Exception("SLYMEDB_TEST_CONNECT_STRING must be set for testing")
        
        # Create database
        store = Store(connectstring)
        store.drop()
        store.create()
        
        jrtable = store.jobreport_table
        
        # Store the job report
        store.save(jobreports)
        
        # Read it back out
        select = jrtable.select().where(jrtable.c.JobID == jobid)
        results = select.execute()
        row = results.fetchone()
        # Cputime matches jobid
        self.assertTrue(row[0] == jobid)
        self.assertTrue(row[7] == Slurm.slurm_time_interval_to_seconds(oldcputime), "Time is %s" % str( Slurm.slurm_time_interval_to_seconds(oldcputime)))
        
        # Save a replacement
        currentrunsh = FakeRunSh(replacement)
        jobreports = Slurm.getJobReports(execfunc = currentrunsh.runsh_i)
        store.save(jobreports)
        # Read it back out
        select = jrtable.select().where(jrtable.c.JobID == jobid)
        results = select.execute()
        row = results.fetchone()
        self.assertTrue(row[0] == jobid)
        self.assertTrue(row[7] == Slurm.slurm_time_interval_to_seconds(newcputime), "Time is %s" % str( Slurm.slurm_time_interval_to_seconds(newcputime)))
        

    
    def testJobWithPipes(self):
        text="""
11508264|lassance|samtools view IMR_PO_051214.bam | awk '{print }' | sort -u -z > regions|FAILED|interact|1|1|00:00:38|00:00.006|00:00.001|00:00.004|2000Mn|2576K|2014-06-11T11:33:55|2014-06-11T11:34:33|holy2a18205|00:00:38       
        """.translate(None,"\n")
        # Generate job reports
        currentrunsh = FakeRunSh(text)        
        jobreports = Slurm.getJobReports(execfunc = currentrunsh.runsh_i)
        
        connectstring = os.environ.get('SLYMEDB_TEST_CONNECT_STRING')
        if not connectstring:
            raise Exception("SLYMEDB_TEST_CONNECT_STRING must be set for testing")
        
        # Create database
        store = Store(connectstring)
        store.drop()
        store.create()
        
        store.save(jobreports)


    def testJobReportLoading(self):
        
        text="""
10048462|akitzmiller|bash|CANCELLED by 0|interact|1|1|02:08:33|08:01.433|06:47.955|01:13.477|2000Mn|2409232K|2014-05-01T11:43:26|2014-05-01T13:51:59|holy2a18206|00:00:10
10053213|akitzmiller|bash|COMPLETED|interact|1|1|01:04:49|13:29.616|11:09.280|02:20.336|20000Mn|468500K|2014-05-01T13:52:30|2014-05-01T14:57:19|holy2a18206|00:00:10
10058675|akitzmiller|bash|CANCELLED by 100278|bigmem|0|2|00:00:00|00:00:00|00:00:00|00:00:00|300000Mn||2014-05-01T17:26:17|2014-05-01T17:26:17|None assigned|00:00:00
10101624|akitzmiller|agalmatest.sbatch|FAILED|bigmem|8|1|00:30:08|03:37.192|02:57.685|00:39.506|300000Mn||2014-05-04T10:34:55|2014-05-04T10:38:41|holybigmem08|00:00:10
10101624.batch||batch|FAILED||1|1|00:03:46|03:37.192|02:57.685|00:39.506|300000Mn|2407896K|2014-05-04T10:34:55|2014-05-04T10:38:41|holybigmem08|00:00:10
10102688|akitzmiller|dusage.sbatch|FAILED|general|4|1|00:01:12|00:00.367|00:00.103|00:00.263|1000Mc||2014-05-02T10:53:11|2014-05-02T10:53:29|holy2a02102|00:00:10
10102688.batch||batch|FAILED||1|1|00:00:18|00:00.367|00:00.103|00:00.263|1000Mc|4260K|2014-05-02T10:53:11|2014-05-02T10:53:29|holy2a02102|00:00:10
10102746|akitzmiller|dusage.sbatch|FAILED|general|4|1|00:00:16|00:00.232|00:00.089|00:00.142|1000Mc||2014-05-02T11:01:08|2014-05-02T11:01:12|holy2a05206|00:00:10
10102746.batch||batch|FAILED||1|1|00:00:04|00:00.232|00:00.089|00:00.142|1000Mc|14340K|2014-05-02T11:01:08|2014-05-02T11:01:12|holy2a05206|00:00:10
10102801|akitzmiller|bash|COMPLETED|interact|1|1|03:24:41|00:18.743|00:08.706|00:10.036|2000Mn|26968K|2014-05-02T11:05:42|2014-05-02T14:30:23|holy2a18206|00:00:10
10213033|akitzmiller|bash|COMPLETED|interact|1|1|01:27:02|00:03.319|00:01.623|00:01.695|2000Mn|5656K|2014-05-05T14:20:40|2014-05-05T15:47:42|holy2a18208|00:00:10
10384435|akitzmiller|agalmatest.sbatch|FAILED|general|8|1|00:01:36|00:01.590|00:01.220|00:00.369|30000Mn|16?|2014-05-09T16:01:53|2014-05-09T16:02:05|holy2a04307|00:00:10
10384435.batch||batch|FAILED||1|1|00:00:12|00:01.590|00:01.220|00:00.369|30000Mn|7068K|2014-05-09T16:01:53|2014-05-09T16:02:05|holy2a04307|00:00:10
10703867|akitzmiller|bash|COMPLETED|interact|1|1|00:07:46|00:01.874|00:01.564|00:00.309|1000Mn|33084K|2014-05-14T14:08:31|2014-05-14T14:16:17|holy2a18205|00:00:10
"""        
        # Generate job reports
        currentrunsh = FakeRunSh(text)        
        jobreports = Slurm.getJobReports(execfunc = currentrunsh.runsh_i)
        
        connectstring = os.environ.get('SLYMEDB_TEST_CONNECT_STRING')
        if not connectstring:
            raise Exception("SLYMEDB_TEST_CONNECT_STRING must be set for testing")
        
        # Create database
        store = Store(connectstring)
        store.drop()
        store.create()
        
        store.save(jobreports)
            
        # Make sure they go in there
        jrtable = store.jobreport_table
        select = jrtable.select().where(jrtable.c.JobID == '10102801')
        results = select.execute()
        row = results.fetchone()
        print row.keys()
        self.assertTrue(row['JobID'] == '10102801')
        self.assertTrue(row['ReqMem_MB_total'] == 2000)
        self.assertTrue(row['MaxRSS_MB'] == 26, "MaxRSS_MB is %d" % row['MaxRSS_MB'])
        self.assertTrue(row['Mem_Wasted'] == 7600, "Mem_Wasted is %d" % row['Mem_Wasted'])
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testJobReportLoading']
    unittest.main()