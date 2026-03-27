import pytest
import allure

@allure.feature("CLI")
@allure.story("Help Command")
@allure.title("Verify greenmask help command")
def test_greenmask_help(greenmask_cmd):
    """Verifies that greenmask help command runs successfully.
    """
    result = greenmask_cmd(["--help"])
    assert "Usage:" in result.stdout
    assert result.returncode == 0

@allure.feature("CLI")
@allure.story("Transformers")
@allure.title("Verify greenmask list-transformers command")
def test_greenmask_list_transformers(greenmask_cmd):
    """Verifies that greenmask list-transformers command runs successfully.
    """
    result = greenmask_cmd(["list-transformers"])
    assert result.returncode == 0
    assert "RandomPerson" in result.stdout
