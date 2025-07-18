from fat32 import Disk

if __name__ == "__main__":
    name = "/dev/disk4"
    with open(name, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block)
        disk.init()

        for file in disk.list_root_files():
            if file["size"] > 0 and file["start_cluster"] > 0 and not file["is_lfn"]:
                print(f"File read in chunks: {file['name']}")

                for chunk in disk.read_file_in_chunks(file):
                    chunk.decode("ascii", errors="replace")
