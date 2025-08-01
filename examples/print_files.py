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
    with open(name, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block, None)
        disk.init()

        print("\nDisk Partitions:\n")
        pp([p.to_dict() for p in disk.partitions])
        print("\nFirst Partition Bios Parameter Block:\n")
        pp(disk.bios_parameter_block.to_dict())
        print()

        for files in disk.list_root_files():
            for file in files:
                if file.size > 0 and file.start_cluster > 0 and not file.is_lfn:
                    print(f"Reading file in chunks: {file.name}\n")

                    for chunk in disk.read_file_in_chunks(file):
                        print(chunk.decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()
