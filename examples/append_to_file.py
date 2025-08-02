import sys
import os
from pprint import pp

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from helper import get_disk, parse_args


def main(path):
    with get_disk(path) as disk:
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
        pp(test.to_dict())


if __name__ == "__main__":
    path = parse_args()
    main(path)
