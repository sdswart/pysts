import os

def data_to_bytes(data):
    if isinstance(data, str):
        if os.path.isfile(data):
            with open(data, 'rb') as f:
                data = f.read()
        else:
            data=data.encode()

    return data
