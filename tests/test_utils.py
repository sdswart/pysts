import pytest

from pysts.utils.utils import iterable

class TestClass:
    '''
    Insure iterable is iterable
    '''
    def test_iterable(self):
        assert iterable(None) is False, 'None is not iterable!'
        assert iterable([1,2,3]), '[1,2,3] is iterable!'
