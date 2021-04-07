import pytest

from utils import iterable

def test_iterable():
    '''
    Insure iterable is iterable
    '''
    assert iterable(None) is False, 'None is not iterable!'
    assert iterable([1,2,3]), '[1,2,3] is iterable!'
