import tempfile
import os
import shutil
import pytest

TEST_IMG = "./tests/data/drive.img"


@pytest.fixture(scope="function")
def drive():
    with tempfile.NamedTemporaryFile(suffix=".img", delete=False) as temp_img:
        temp_img_path = temp_img.name

    shutil.copyfile(TEST_IMG, temp_img_path)

    os.chmod(temp_img_path, 0o666)

    yield temp_img_path

    os.remove(temp_img_path)
