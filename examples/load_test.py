import sys
import os
import random
import string

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from helper import get_disk, parse_args


def main(path):
    with get_disk(path) as disk:
        disk.init()

        for i in range(0, 100):
            file = disk.create_file(f"test-{i}")
            random_text = "".join(
                random.choices(string.ascii_letters + string.digits, k=6500)
            )
            for _ in range(10):
                data = bytearray(f"{random_text}\n", "ascii")
                file = disk.append_to_file(file, data)

    count = 0
    read_files = []
    for files in disk.list_root_files():
        for file in files:
            read_files.append(file.to_dict())
        count += len(files)

    size = sum(
        file["size"] for file in read_files if file.get("name", "").startswith("test")
    )

    print(f"{size} bytes written across {count} files.")


if __name__ == "__main__":
    path = parse_args()
    main(path)
