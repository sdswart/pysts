import dash

def get_dynamic_values(id_type):
    res={}
    for inputs_list in dash.callback_context.inputs_list:
        if isinstance(inputs_list,list): #dynamic types
            for cur_input in inputs_list:
                if 'value' in cur_input and isinstance(cur_input['id'],dict) and cur_input['id']['type']==id_type:
                    res[cur_input['id']['index']]=cur_input['value']
    return res
