from . import db, classes

get_oracle_conn = lambda user,password,host,port,server: f'oracle+cx_oracle://{user}:{password}@{host}:{port}/{server}'
oracle_connections={'buildware':dict(user='gpms_read',password='readonly',host='ackbar',port='1521',server='prod112'),
                    'halifax':dict(user='gpms_read',password='readonly',host='tie',port='1521',server='shwtr102')}
oracle_schemas={
                'buildware':'gpms_owner',
                'halifax':'gpms_owner',
                }




class Analyzer(classes.DB):
    def __init__(self,db_params=None,schema=None):
        if isinstance(db_params,str):
            assert db_params in oracle_connections, f"{db_params} is not available in standard definitions, please provide the user, password, host, port and server instead."
            if schema is None and db_params in oracle_schemas:
                schema=oracle_schemas[db_params]
            db_params=oracle_connections[db_params]
        assert isinstance(db_params,dict), f"A dictionary of the databe connection is required. Instead recieved db as {type(db_params)}"
        conn_params=['user', 'password', 'host', 'port', 'server']
        missing_params=[x for x in conn_params if x not in db_params]
        assert len(missing_params)==0, f"Missing connection parameters: {', '.join(missing_params)}"
        self.db_params={key:val for key,val in db_params.items() if key in conn_params}
        conn_str=get_oracle_conn(**self.db_params)
        if schema is None:
            schema=self.db_params['user']
        super().__init__(conn_str,schema=schema)
