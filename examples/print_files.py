import sys
import os
from pprint import pp

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from helper import get_disk, parse_args


def main(path):
    with get_disk(path) as disk:
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
    path = parse_args()
    main(path)
