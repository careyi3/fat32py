import sys
import os
import tempfile
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pprint import pp
from fat32 import Disk

TEST_IMG = "./tests/data/drive.img"


def drive():
    with tempfile.NamedTemporaryFile(suffix=".img", delete=False) as temp_img:
        temp_img_path = temp_img.name

    shutil.copyfile(TEST_IMG, temp_img_path)

    os.chmod(temp_img_path, 0o666)

    yield temp_img_path

    os.remove(temp_img_path)


def main():
    name = next(drive())
    with open(name, "rb+") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        def write_block(logical_block_address, data):
            f.seek(logical_block_address * 512)
            return f.write(data)

        disk = Disk(read_block, write_block)
        disk.init()

        to_write = None
        for files in disk.list_root_files():
            for file in files:
                if file.name == "LOG-1":
                    to_write = file
                    break

        s = ""
        for chunk in disk.read_file_in_chunks(to_write):
            s += chunk.decode("ascii", errors="replace")
        print(f"Original content of: {to_write.name}")
        print("```")
        print(s)
        print("```")
        print("File details:")
        pp(to_write.to_dict())
        print()

        disk.append_to_file(to_write, bytearray(bytes("Test Data", "ascii")))
        assert disk.reads == 3
        assert disk.writes == 2

        test = None
        for files in disk.list_root_files():
            for file in files:
                if file.name == "LOG-1":
                    test = file
                    break

        s = ""
        for chunk in disk.read_file_in_chunks(test):
            s += chunk.decode("ascii", errors="replace")
        print(f"Updated content of: {test.name}")
        print("```")
        print(s)
        print("```")
        print("File details:")
        pp(to_write.to_dict())


if __name__ == "__main__":
    main()
