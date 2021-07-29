import pytest

from subprocess import Popen, PIPE
import time
import requests
import os

from pysts.utils.utils import kill_proc_tree

CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008
max_wait_seconds=30

class TestClass:
    '''
    Test the dash server
    '''
    def test_server(self):
        path=os.path.join(os.path.dirname(os.path.realpath(__file__)),'test_dash.py')
        p=Popen(['python', path], stdin=PIPE, stdout=PIPE, stderr=PIPE)

        start_time = time.time()
        site_found=False
        msg=''
        counter=0
        delta_seconds=0
        while not site_found and delta_seconds<max_wait_seconds:
            delta_seconds = int(time.time() - start_time)
            #Check if the process is running
            assert p.poll() is None, f'Sever failed to start with:\nSTDOUT = {p.stdout.read().decode()}\nSTDERR = {p.stderr.read().decode()}'

            #Check if the server url is available
            try:
                r = requests.get('http://localhost:5000/')
                site_found = r.status_code==200
                msg=f'Request succeeded but with code: {r.status_code}!'
                break
            except requests.exceptions.Timeout:
                msg='Request timed out!'
            except requests.exceptions.TooManyRedirects:
                msg='Too many redirects!'
            except requests.exceptions.RequestException as e:
                msg='Server not found!'
            counter+=1
            time.sleep(1)

        if p.poll() is None:
            kill_proc_tree(p.pid)
        assert site_found, f"Sever took longer than {max_wait_seconds} seconds to start after {counter} tries, with the last error:\n{msg}"

if __name__ == '__main__':
    from pysts.dash import components, start_app
    class Main(components.Component):
        def layout(self):
            self.full_layout=components.base.html.Div('Test Dash server')
            return self.full_layout
    start_app(Main())
