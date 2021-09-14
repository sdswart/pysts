import logging
import random
from datetime import datetime, timedelta
import pytz

from influxdb_client import InfluxDBClient, BucketRetentionRules, Point, WriteOptions
from influxdb_client.extras import pd, np

from pysts.utils.utils import create_logger

logger = create_logger(__name__) #pysts.db.influxdb.classes

def logging_for_dataframe_serializer(enable=True):
    loggerSerializer = logging.getLogger('influxdb_client.client.write.dataframe_serializer')
    if enable:
        loggerSerializer.setLevel(level=logging.DEBUG)
        if len(loggerSerializer.handlers)==0:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
            loggerSerializer.addHandler(handler)
    else:
        loggerSerializer.setLevel(level=logging.ERROR)

def remove_tzinfo(dt):
    if isinstance(dt,datetime):
        dt=dt.replace(tzinfo=None)
    return dt

def add_tzinfo(dt):
    if isinstance(dt,datetime):
        if dt.tzinfo is None:
            dt=dt.replace(tzinfo=pytz.UTC)
    return dt

def datetime_to_RFC3339(dt):
    if isinstance(dt,datetime):
        dt=add_tzinfo(dt).isoformat()
    return dt

class InfluxDB(object):
    _client=None
    def __init__(self,url = 'http://l3-37:8086',
                token = "nH1_WXXTr03i1SKqxDdPU9Y5RD0C2fTH82t1zUS47-WlE4Re8Dt31WF-YhTnNnSxTIA4G803bkbDkOV67atsaw==",
                org = 'Gastops',
                timeout=999999
        ):
        for attr in ['url','token','org','timeout']:
            setattr(self,attr,locals()[attr])

    @property
    def client(self):
        if self._client is None:
            self._client=InfluxDBClient(url=self.url, token=self.token, org=self.org,timeout=self.timeout)
        return self._client

    def create_bucket(self,name,retention_seconds=None):
        retention_rules = None if retention_seconds is None else BucketRetentionRules(type="expire", every_seconds=retention_seconds)
        created_bucket = self.client.buckets_api().create_bucket(bucket_name=name,retention_rules=retention_rules,org=self.org)
        return created_bucket

    def get_bucket(self,name, create_if_not_found=True, retention_seconds=None):
        bucket=self.client.buckets_api().find_bucket_by_name(name)
        if bucket is None and create_if_not_found:
            bucket=self.create_bucket(name,retention_seconds=retention_seconds)
        return bucket
    def bucket_exists(self,name):
        return name in self.list_bucket_names()
    def delete_bucket(self,name):
        self.client.buckets_api().delete_bucket(self.get_bucket(name))
    def list_buckets(self):
        return self.client.buckets_api().find_buckets().buckets
    def list_bucket_names(self):
        return [x.name for x in self.list_buckets()]

    def get_data(self,bucket_name,*args,return_dataframe=True,as_stream=False,**kwargs):
        query=self.get_flux(bucket_name,*args,return_dataframe=return_dataframe,**kwargs)
        logger.debug(f'Executing query: {query}')
        query_api = self.client.query_api()
        if return_dataframe:
            return query_api.query_data_frame(query)
        elif as_stream:
            return query_api.query_stream(query)
        else:
            return [record.values for table in query_api.query(query) for record in table.records]

    def get_flux(self,bucket_name,*args,measurements=None,tags=None,
                start='0',stop=None,return_dataframe=True,keep_keys=None,
                keep_fields=None,limit=None,extra_flux_commands=None,**kwargs):
        """
            args can be used to check if a _field exists
            kwargs can be used to check if a _field is equal to the given _value
        """
        start=datetime_to_RFC3339(start)
        stop=datetime_to_RFC3339(stop)
        flux=[
            f'from(bucket:"{bucket_name}")',
            f' |> range(start: {start}, stop: {stop})' if stop is not None else f' |> range(start: {start})'
        ]

        filters=[]
        def get_filter(key,vals):
            if type(vals) not in [list,tuple,np.ndarray]:
                vals=[vals]
            res=[]
            for val in vals:
                if isinstance(val,str):
                    if not any([val.strip().startswith(x) for x in ['=','>','<']]):
                        val=f'r.{key} == "{val}"'
                else:
                    val=f'r.{key} == {val}'
                res.append(val)
            return ("(" if len(res)>1 else "") + " or ".join(res) + (")" if len(res)>1 else "")
        if measurements is not None:
            filters.append(get_filter('_measurement',measurements))
        if tags is not None:
            filters.append(get_filter('tag',tags))
        if keep_fields is not None:
            if type(keep_fields) not in [list,tuple,np.ndarray]: keep_fields=[keep_fields]
            args=list(args)+keep_fields
        kwargs.update({x:None for x in args if x not in kwargs})
        field_filters=[]
        if len(kwargs)>0:
            for key,val in kwargs.items():
                cur_filter=get_filter('_field',key)
                if val is not None:
                    cur_filter=f'({cur_filter} and {get_filter("_value",val)})'
                field_filters.append(cur_filter)
        if len(field_filters)>0:
            filters.append(("(" if len(field_filters)>1 else "")+" or ".join(field_filters)+(")" if len(field_filters)>1 else ""))
        if len(filters)>0:
            flux.append(f' |> filter(fn: (r) => {" and ".join(filters)})')

        if extra_flux_commands is not None:
            if type(extra_flux_commands) not in [list,tuple,np.ndarray]: extra_flux_commands=[extra_flux_commands]
            extra_flux_commands=[f'{"" if x.startswith(" ") else " "}{"" if x.strip().startswith("|>") else "|> "}{x}' for x in extra_flux_commands]
            flux.extend(extra_flux_commands)



        if return_dataframe:
            flux.append(' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')
        elif keep_keys is not None:
            if type(keep_keys) not in [list,tuple,np.ndarray]: keep_keys=[keep_keys]
            keep_keys=[f'"{x}"' for x in keep_keys]
            flux.append(f' |> keep(columns: [{", ".join(keep_keys)}])')

        if limit is not None:
            flux.append(f' |> limit(n:{limit}, offset: 0)')

        return "".join(flux)

    def get_time_range(self,bucket_name,**kwargs):
        start_vals=self.get_data(bucket_name,return_dataframe=False,keep_keys=['_time'],extra_flux_commands='first(column: "_time")',**kwargs)
        stop_vals=self.get_data(bucket_name,return_dataframe=False,keep_keys=['_time'],extra_flux_commands='last(column: "_time")',**kwargs)
        if len(start_vals)>0 and len(stop_vals)>0:
            return min([x['_time'] for x in start_vals]),max([x['_time'] for x in stop_vals])

    def add_data(self,bucket_name,data,measurement,time_col=None,tag_columns=None,show_logs=False,
                    write_options=None,batch_size=None, flush_interval=None, remove_existing_times=True):
        """
            Example write_options = dict(batch_size=50_000, flush_interval=10_000)
        """

        logging_for_dataframe_serializer(show_logs)
        if isinstance(data,pd.DataFrame):
            if data.shape[0]==0:
                return

            #Make sure the index is time
            index=data.index
            if index.name is None or not isinstance(index[0],datetime):
                if time_col is None:
                    cols=[x for x in data.columns if isinstance(data[x].iloc[0],datetime)]
                    assert len(cols)==1, 'Could not identify the time column in the dataframe. Please set it as the index or provide the label of the column as the variable time_col'
                    time_col=cols[0]
                data=data.set_index(time_col)

            data.index=data.index.map(add_tzinfo)

            if remove_existing_times:
                t_range=self.get_time_range(bucket_name,measurements=measurement,start=data.index.min(),stop=data.index.max())
                if t_range is not None:
                    data=data[(data.index<t_range[0]) | (data.index>t_range[1])]
            num_points=data.shape[0]
        else:
            if not type(data) in [tuple,list]:
                data=[data]
            assert isinstance(data[0],dict) and len(data[0])>0, "Data should be one of: dict, list of dicts or a pandas dataframe"

            if time_col is None:
                cols=[key for key,val in data[0].items() if isinstance(val,datetime)]
                assert len(cols)==1, 'Could not identify the time column in the data. Please set it as the index or provide the label of the column as the variable time_col'
                time_col=cols[0]
            if tag_columns is None:
                tag_columns=[]
            if not type(tag_columns) in [list,tuple]:
                tag_columns=[tag_columns]
            points=[]
            t_range=None
            if remove_existing_times:
                data_times=np.array([x[time_col] for x in data])
                t_range=self.get_time_range(bucket_name,measurements=measurement,start=data_times.min(),stop=data_times.max())
            for datum in data:
                datum[time_col]=add_tzinfo(datum[time_col])
                if t_range is None or datum[time_col]<t_range[0] or datum[time_col]>t_range[1]:
                    tags={} if len(tag_columns)==0 else {'tags':{key:datum.pop(key) for key in tag_columns if key in datum}}
                    points.append(Point.from_dict({'measurement':measurement,'time':datum.pop(time_col),'fields':datum,**tags}))
            data=points
            num_points=len(data)

        if num_points>0:
            if write_options is None:
                write_options={}
            if batch_size is not None:
                write_options['batch_size']=batch_size
            elif num_points>5000: #Ideal = 5000 to 10000 - use 10000
                write_options['batch_size']=min([num_points,10000])

            if flush_interval is not None:
                write_options['flush_interval']=flush_interval
            options={} if len(write_options)==0 else {'write_options':WriteOptions(**write_options)}
            logger.debug(f'Writing data with {num_points} points to bucket: {bucket_name}, measurement: {measurement}, with write_options: {write_options}')
            with self.client.write_api(**options) as write_api:
                write_api.write(bucket=bucket_name, record=data,
                            data_frame_tag_columns=tag_columns,
                            data_frame_measurement_name=measurement)
