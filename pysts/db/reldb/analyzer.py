from .db import *
from .classes import *

get_oracle_conn = lambda user,password,host,port,server: f'oracle+cx_oracle://{user}:{password}@{host}:{port}/{server}'
oracle_connections={'repower':dict(user='tracker_repower',password='tracker_repower',host='leia',port='1522',server='ora112'),
                    'tracker':dict(user='tracker_web',password='tracker_web',host='leia',port='1522',server='ora112'),
                    'oilat':dict(user='tracker_read',password='readonly',host='dooku',port='1521',server='nlsvc112')}
oracle_schemas={'oilat':'tracker_pwc',
                'tracker':'tracker_web',
                'repower':'tracker_repower',
                }

#Build-types-makes
def get_build_type_filters(tables):
    renames={'short_desc':'type','nomenclature':'make','description':'description','assembly_name':'model'}
    cols= [
        tables.slot.columns.slot_id,
        tables.slot.columns.nomenclature,
        tables.cdtbl_slot_class.columns.short_desc,
        tables.assembly.columns.assembly_name,
        tables.assembly.columns.description
    ]
    filters=[
        tables.slot.columns.assembly_id==tables.assembly.columns.assembly_id,
        tables.cdtbl_slot_class.columns.slot_class_cd==tables.slot.columns.slot_class_cd
    ]
    return cols,filters,renames

#organization functions
def organization_props(self):
    return {'name':self.organization_name,
        'type':self.get_org_type_cd[0].short_desc,
        'location_name':self.location_name,
        'street_address':self.street_address,
        'city':self.city,
        'state_province':self.state_province,
        'country':self.country,
        'postal_code':self.postal_code}

def organization_items(self,tables):
    return self.search(tables.inv_item).filter(
        tables.inv_item.columns.org_id==self.org_id,
        tables.inv_item.columns.sn==tables.inv_item.columns.h_sn,
        tables.inv_item.columns.nh_sn==None
    )

def org_parents(self,tables):
    return self.search(tables.organization).filter(
        tables.organization.columns.nh_org_id==self.org_id
    )

def organization_site_items(self,tables):
    return self.sess.query(
        tables.organization.organization_name,
        tables.inv_item
    ).filter(
        tables.inv_item.columns.org_id==tables.organization.org_id,
        tables.organization.columns.nh_org_id==self.org_id
    )

#inv_item functions
def inv_item_props(self):
    part_x_slot=self.get_slot_id[0]
    slot=part_x_slot.get_slot_id[0] # inv_item->part_x_slot->slot_id
    part=part_x_slot.get_part_id[0] # inv_item->part_x_slot->part_id
    return {'name':self.user_serial_no,
        'build':slot.build,
        'manufacturer':part.mfr_name,
        'manufacturer_part_no':part.mfr_part_no,
        'description':part.nomenclature}

def inv_item_data(self,tables):
    sc_sample=orm.aliased(tables.samp_collect)
    sc_test=orm.aliased(tables.samp_collect)
    return self.sess.query(
            tables.inv_item.columns.user_serial_no,
            sc_sample.columns.sample_cd,
            sc_sample.columns.sample_date,
            tables.pdm_samp_type.columns.samp_name,
            tables.data_collect_result.columns.data_value,
            tables.cdtbl_analy_method.columns.short_desc,
            tables.cond_ind.columns.ind_name,
            tables.cond_ind.columns.ind_desc,
            tables.cond_ind.columns.ind_symbol
        ).filter(
            tables.inv_item.columns.sn==self.sn,
            sc_sample.columns.delete_flg == 'N',
            sc_sample.columns.sn==self.sn,
            sc_sample.columns.samp_ident_no==sc_sample.columns.h_samp_ident_no,
            sc_test.columns.h_samp_ident_no==sc_sample.columns.samp_ident_no,
            sc_test.columns.samp_ident_no != sc_sample.columns.samp_ident_no,
            sc_test.columns.samp_type_no == tables.pdm_samp_type.columns.samp_type_no,
            sc_test.columns.samp_ident_no == tables.data_collect_result.columns.samp_ident_no,
            tables.data_collect_result.columns.ind_no == tables.cond_ind.columns.ind_no,
            tables.cdtbl_analy_method.columns.analy_method_cd==tables.data_collect_result.columns.analy_method_cd
        ).order_by(sc_sample.columns.sample_date.asc())

def inv_item_limits(self,tables):
    return self.sess.query(
            tables.slot.columns.slot_id,
            tables.slot.columns.nomenclature,
            tables.limit_def.columns.lower_limit,
            tables.cond_ind_status_def.columns.status_name,
            tables.cond_ind.columns.ind_name,
            tables.cond_ind.columns.ind_symbol,
            tables.cond_ind.columns.ind_desc
        ).filter(
            tables.inv_item.columns.sn==self.sn,
            tables.slot.columns.slot_id==tables.inv_item.columns.slot_id,
            tables.limit_def.columns.object_id==tables.slot.columns.slot_id,
            tables.cond_ind.columns.ind_no==tables.pdm_samp_cond_ind.columns.ind_no,
            tables.pdm_samp_cond_ind.columns.ind_no==tables.cond_ind_status_def.columns.ind_no,
            tables.cond_ind_status_def.columns.status_no==tables.limit_def.columns.status_no
        )

def inv_item_components(self,tables):
    return self.search(
        tables.inv_item
    ).filter(
        tables.inv_item.columns.sn==tables.inv_item.columns.h_sn,
        tables.inv_item.columns.nh_sn==self.sn
    )

def inv_item_other_items(self,tables):
    return self.search(
        tables.inv_item
    ).filter(
        tables.inv_item.columns.h_sn==self.sn,
        tables.inv_item.columns.nh_sn==self.sn
    )

def inv_item_sensors(self,tables):
    return self.search(
        tables.inv_item
    ).filter(
        tables.inv_item.columns.h_sn==self.sn,
        tables.inv_item.columns.nh_sn==self.sn,
        tables.slot.columns.slot_class_cd=='LN',
        tables.inv_item.columns.slot_id==tables.part_x_slot.columns.slot_id,
        tables.part_x_slot.columns.slot_id==tables.slot.columns.slot_id
    ).distinct()

#slot
def slot_build_type(self,tables):
    cols,filters,renames=get_build_type_filters(tables)
    filters.append(tables.slot.columns.slot_id==self.slot_id)
    builds=self.sess.query(*cols).filter(*filters).all()
    res=[{rename:getattr(build,key) for key,rename in renames.items()} for build in builds]
    return res[0] if len(res)==1 else res

def get_table_attribs(tables):
    return {'organization':{
            'props':organization_props,
            'sites':lambda self: self.search(tables.organization).filter(tables.organization.columns.nh_org_id==self.org_id).distinct(),
            'parents':lambda self: org_parents(self,tables=tables),
            'items':lambda self: organization_items(self,tables=tables),
            'site_items':lambda self: organization_site_items(self,tables=tables)
        },
        'inv_item':{
            'organization':lambda self: self.get_org_id[0],
            'props': inv_item_props,
            'data':lambda self: query_to_df(inv_item_data(self,tables=tables)),
            'limits': lambda self: inv_item_limits(self,tables=tables),
            'components':lambda self: inv_item_components(self,tables=tables),
            'other':lambda self: inv_item_other_items(self,tables=tables),
            'sensors':lambda self: inv_item_sensors(self,tables=tables)
        },
        'slot':{
            'build': lambda self: slot_build_type(self,tables=tables),
        }
    }

class Analyzer(DB):
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
        super().__init__(conn_str,schema=schema,
                        table_names=['cond_ind','slot','part','assembly','data_collect_result','inv_item','organization','samp_collect',
                                    'pdm_samp_type','contacts','cond_ind_status_def','limit_def','cdtbl_org_type','cdtbl_unit_of_meas',
                                    'cdtbl_meas_type','pdm_samp_cond_ind','cdtbl_analy_method','cdtbl_present_format','part_x_slot','cdtbl_object_type'
        ])
    def __str__(self):
        connected="connected" if hasattr(self,'sess') else "not connected"
        return f'{self.db_params["server"]} on {self.db_params["host"]} with schema {self.schema} ({connected})'
    def connect(self,*args,**kwargs):
        self=super().connect(*args,**kwargs)

        extra_attribs=get_table_attribs(self.tables)
        for table_name,attribs in extra_attribs.items():
            self.tables.__dict__[table_name].add_result_attribs(attribs)
        return self
    @property
    def props(self):
        if hasattr(self,'sess'):
            return {
                'orgs':self.tables.organization.objects.count(),
                'items':self.tables.inv_item.objects.count()
            }
        else:
            return 'Not connected'
    @property
    def fields(self):
        return self.sess.query(
                self.tables.pdm_samp_type.columns.samp_name,
                self.tables.cond_ind.columns.ind_name,
                self.tables.cond_ind.columns.ind_desc,
                self.tables.cond_ind.columns.ind_symbol
            ).filter(
                self.tables.samp_collect.columns.samp_type_no == self.tables.pdm_samp_type.columns.samp_type_no,
                self.tables.samp_collect.columns.samp_ident_no == self.tables.data_collect_result.columns.samp_ident_no,
                self.tables.data_collect_result.columns.ind_no == self.tables.cond_ind.columns.ind_no
            ).distinct()
    @property
    def fields_df(self):
        return query_to_df(self.fields)
    def all_limits(self):
        tables=self.tables
        cols,filters,renames=get_build_type_filters(tables)
        cols=[
            tables.limit_def.columns.lower_limit,
            tables.cdtbl_object_type.columns.object_type_name,
            tables.limit_def.columns.is_single_value_flg,
            tables.cond_ind_status_def.columns.status_name,
            tables.cond_ind.columns.ind_name,
            tables.cond_ind.columns.ind_symbol,
            tables.cond_ind.columns.ind_desc
        ]+cols
        filters=[
            tables.slot.columns.slot_id==tables.limit_def.columns.object_id,
            tables.cdtbl_object_type.columns.object_type==tables.limit_def.columns.object_type,
            tables.cond_ind.columns.ind_no==tables.pdm_samp_cond_ind.columns.ind_no,
            tables.pdm_samp_cond_ind.columns.ind_no==tables.cond_ind_status_def.columns.ind_no,
            tables.cond_ind_status_def.columns.status_no==tables.limit_def.columns.status_no
        ]+filters
        return self.sess.query(*cols).filter(*filters).distinct()
    @property
    def limits(self):
        _,_,renames=get_build_type_filters(self.tables)
        df=query_to_df(self.all_limits())
        df=df.rename(columns=renames)
        build_cols=list(renames.values())
        df=df.set_index(build_cols).reorder_levels(build_cols).sort_index()
        return df
