# code imports
import pytest

# define tests
class TestDummy:

	def test_case1(self):
		x=5
		y=5
		assert x == y,'example test failed'

	@pytest.mark.parametrize('x, y',[(1, 1),(2, 2)])
	def test_case2(self, x, y):
		assert x == y,'example test failed'
	
	@pytest.mark.xfail #skip is another option (@pytest.mark.skip)
	def test_case3(self, dummy_fixture):
		assert dummy_fixture[0] == 1
		assert dummy_fixture[1] == 20
		assert dummy_fixture[2] == 3