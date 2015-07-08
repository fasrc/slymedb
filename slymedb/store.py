'''
Created on May 19, 2014

@author: aaronkitzmiller
'''
import re
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Column, Table, types
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import sessionmaker


class Store(object):
    """
    Default database store for slyme-db.  Uses SQLAlchemy to perform 
    persistence operations
    """
    
    def __init__(self,connectstring):
        """
        Create the engine and connection.  Define the jobreport table
        """       
        # configure Session class with desired options
        self.engine = create_engine(connectstring)
        Session = sessionmaker(bind=self.engine)

        # work with the session
        self.session = Session()
        self.metadata = MetaData()
        
        self.jobreport_table = Table('jobreport', self.metadata, 
            Column('JobID',      types.String(20), nullable=False, unique=True),
            Column('User',       types.String(50)),
            Column('JobName',    types.String(255)),
            Column('State',      types.String(20)),
            Column('Partition',  types.String(255)),
            Column('NCPUS',      types.Integer),
            Column('NNodes',     types.Integer),
            Column('CPUTime',    types.Float),
            Column('TotalCPU',   types.Float),
            Column('UserCPU',    types.Float),
            Column('SystemCPU',  types.Float),
            Column('MaxRSS_kB',  types.BigInteger),
            Column('ReqMem_bytes_per_node',  types.BigInteger),
            Column('ReqMem_bytes_per_core',  types.BigInteger),
            Column('ReqMem_bytes',           types.BigInteger),
            Column('Start',      types.DateTime),
            Column('End',        types.DateTime),
            Column('NodeList',   types.String(255)),
            Column('ReqMem_bytes_total',     types.BigInteger),
            Column('CPU_Efficiency',         types.Float),
            Column('CPU_Wasted', types.Float),
            Column('CancelledBy',types.String(20)),
        )
        
        self.rejected_table = Table('rejected', self.metadata, 
            Column('JobID',      types.String(20), nullable=False, unique=True),
            Column('User',       types.String(50)),
            Column('JobName',    types.String(255)),
            Column('State',      types.String(20)),
            Column('Partition',  types.String(255)),
            Column('NCPUS',      types.Integer),
            Column('NNodes',     types.Integer),
            Column('CPUTime',    types.Float),
            Column('TotalCPU',   types.Float),
            Column('UserCPU',    types.Float),
            Column('SystemCPU',  types.Float),
            Column('MaxRSS_kB',  types.BigInteger),
            Column('ReqMem_bytes_per_node',  types.BigInteger),
            Column('ReqMem_bytes_per_core',  types.BigInteger),
            Column('ReqMem_bytes',           types.BigInteger),
            Column('Start',      types.DateTime),
            Column('End',        types.DateTime),
            Column('NodeList',   types.String(255)),
            Column('ReqMem_bytes_total',     types.BigInteger),
            Column('CPU_Efficiency',         types.Float),
            Column('CPU_Wasted', types.Float),
            Column('CancelledBy',types.String(20)),
        )
        
        self.metadata.bind = self.engine
        self.connection = self.engine.connect()
        
    def create(self):
        """
        Actually creates the database tables.  Be careful
        """
        self.metadata.create_all(checkfirst=True)
        
    def drop(self):
        """
        Drop the database table
        """
        self.metadata.drop_all(checkfirst=True)
    
    def fetch(self,**kwargs):
        """
        Retrieve an array of JobReport objects using column values
        """
        pass
    
    def save(self,jobreports,replace=True):
        """
        Store an array of JobReport objects.  Only stores the attributes 
        represented by columns; there can be others that are ignored.
        If replace is rTrue, then an integrity error results in an
        update.
        """
        errorre = re.compile(r'Duplicate entry|UNIQUE constraint failed')
        for jobreport in jobreports:
            vals = {c.name:jobreport[c.name] for c in self.jobreport_table.columns}
            try: 
                insert = self.jobreport_table.insert(values=vals)
                self.connection.execute(insert)
            except Exception, e:
                match = errorre.search(str(e))
                if match is not None:
                    update = self.jobreport_table.update().where(self.jobreport_table.c.JobID == jobreport["JobID"]).values(vals)
                    self.connection.execute(update)
                else:
                    raise e 
            
    
    
