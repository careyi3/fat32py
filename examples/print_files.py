import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pprint import pp
from fat32 import Disk


def main():
    name = "./tests/data/drive.img"
    with open(name, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block, None)
        disk.init()

        print("\nDisk Partitions:\n")
        pp(disk.partitions)
        print("\nFirst Partition Bios Parameter Block:\n")
        pp(disk.bios_parameter_block)
        print()

        for file in disk.list_root_files():
            if file["size"] > 0 and file["start_cluster"] > 0 and not file["is_lfn"]:
                print(f"Reading file in chunks: {file['name']}\n")

                for chunk in disk.read_file_in_chunks(file):
                    print(chunk.decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()
