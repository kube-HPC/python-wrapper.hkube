
import os
import sys
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

    python_path = ":".join(sys.path)[1:]
    environ = {'PYTHONPATH': python_path}
    copyEnv = os.environ.copy()
    copyEnv.update(environ)
    print('python_path: ' + python_path)

    subprocess.Popen(['python', 'tests/mock_ws_server.py'], env=copyEnv)


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
