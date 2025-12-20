@allure.feature("Allure Integration")
@allure.story("Report Generation")
def test_allure_dummy():
    """
    Dummy test to verify Allure report generation.
    """
    with allure.step("Step 1"):
        pass
    with allure.step("Step 2"):
        assert True
    """
    Dummy test to verify Allure report generation.
    """
    with allure.step("Step 1"):
        pass
    with allure.step("Step 2"):
        assert True
