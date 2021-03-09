import os
import tempfile

from gsite import create_app

__, download_dir = tempfile.mkstemp()


def test_config():
    assert not create_app().testing
    assert create_app(
        {
            "TESTING": True,
        }
    ).testing

    os.unlink(download_dir)
