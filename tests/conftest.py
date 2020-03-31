
import subprocess


def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    print('pytest_configure')


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    print('pytest_sessionstart')

    subprocess.call(['python', 'tests/data_server.py'])
    subprocess.call(['python', 'tests/mock_ws_server.py'])


def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    print('pytest_sessionfinish')


def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
    print('pytest_unconfigure')
