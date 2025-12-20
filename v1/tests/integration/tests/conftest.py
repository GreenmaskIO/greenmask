import pytest
import os
import subprocess
import shutil
import logging
import allure

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def greenmask_bin():
    """Locates the greenmask binary.
    Prioritizes GREENMASK_BIN env var.
    """
    bin_path = os.getenv("GREENMASK_BIN")
    if bin_path:
        if not os.path.isfile(bin_path):
             pytest.fail(f"GREENMASK_BIN is set to {bin_path} but it does not exist.")
        return bin_path

    # Assumes greenmask is in PATH if not specified
    bin_path = shutil.which("greenmask")
    if not bin_path:
        pytest.skip("greenmask binary not found in PATH and GREENMASK_BIN is not set.")
    return bin_path

@pytest.fixture
def greenmask_config(request, tmp_path):
    """Sets up the configuration environment.
    Copies the default config to a temp dir to allow tests to modify it if needed,
    or just returns a path to a static config.
    """
    # For now, let's just return the path to the resources config
    # In the future, we might want to copy it to tmp_path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "resources", "config.yaml")
    
    if not os.path.exists(config_path):
        pytest.logger.warning(f"Config file not found at {config_path}")
        
    return config_path

@pytest.fixture
def greenmask_cmd(greenmask_bin, greenmask_config):
    """Executes greenmask commands.
    """
    @allure.step("Run greenmask {args}")
    def _run_greenmask(args, env=None, check=True):
        cmd = [greenmask_bin, "--config", greenmask_config] + args
        
        full_env = os.environ.copy()
        if env:
            full_env.update(env)
            
        logger.info(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=full_env,
        )
        
        if check and result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            allure.attach(result.stdout, name="stdout", attachment_type=allure.attachment_type.TEXT)
            allure.attach(result.stderr, name="stderr", attachment_type=allure.attachment_type.TEXT)
            pytest.fail(f"greenmask command failed: {result.stderr}")
            
        allure.attach(result.stdout, name="stdout", attachment_type=allure.attachment_type.TEXT)
        return result

    return _run_greenmask
