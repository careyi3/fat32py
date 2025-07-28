import tempfile
import os
import shutil
import pytest

TEST_IMG = "./tests/data/drive.img"


@pytest.fixture(scope="function")
def drive():
    temp_img = tempfile.NamedTemporaryFile(suffix=".img", delete=False)
    shutil.copy(TEST_IMG, temp_img.name)

    yield temp_img.name

    os.remove(temp_img.name)
