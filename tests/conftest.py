'''
define fixtures for the test suit
'''
# code imports
import pytest

# define fixtures
@pytest.fixture
def dummy_fixture():
    return [1,2,3]