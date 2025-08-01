import sys
import tempfile
import os
import shutil
import argparse
from contextlib import contextmanager

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fat32 import Disk

TEST_IMG = "./tests/data/drive.img"


@contextmanager
def get_disk(path):
    if path is None:
        with tempfile.NamedTemporaryFile(suffix=".img", delete=False) as temp_img:
            temp_img_path = temp_img.name

        shutil.copyfile(TEST_IMG, temp_img_path)

        os.chmod(temp_img_path, 0o666)

        with open(temp_img_path, "rb+") as f:

            def read_block(logical_block_address):
                f.seek(logical_block_address * 512)
                return f.read(512)

            def write_block(logical_block_address, data):
                f.seek(logical_block_address * 512)
                return f.write(data)

            disk = Disk(read_block, write_block)

            yield disk

        os.remove(temp_img_path)
    else:
        with open(path, "rb+") as f:

            def read_block(logical_block_address):
                f.seek(logical_block_address * 512)
                return f.read(512)

            def write_block(logical_block_address, data):
                f.seek(logical_block_address * 512)
                return f.write(data)

            disk = Disk(read_block, write_block)

            yield disk


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, help="Optional path argument", default=None)
    args = parser.parse_args()

    path = args.path if args.path else None
    return path
