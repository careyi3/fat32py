import pytest
from fat32 import Disk


def test_init(drive):
    with open(drive, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block)
        disk.init()

        partition_table = [
            {
                "boot_flag": 0,
                "end_chs": b"\xfe\xff\xff",
                "num_sectors": 131071,
                "start_chs": b"\xfe\xff\xff",
                "start_lba": 1,
                "type": 11,
            },
            {
                "boot_flag": 0,
                "end_chs": b"\x00\x00\x00",
                "num_sectors": 0,
                "start_chs": b"\x00\x00\x00",
                "start_lba": 0,
                "type": 0,
            },
            {
                "boot_flag": 0,
                "end_chs": b"\x00\x00\x00",
                "num_sectors": 0,
                "start_chs": b"\x00\x00\x00",
                "start_lba": 0,
                "type": 0,
            },
            {
                "boot_flag": 0,
                "end_chs": b"\x00\x00\x00",
                "num_sectors": 0,
                "start_chs": b"\x00\x00\x00",
                "start_lba": 0,
                "type": 0,
            },
        ]

        partition = {
            "boot_flag": 0,
            "end_chs": b"\xfe\xff\xff",
            "num_sectors": 131071,
            "start_chs": b"\xfe\xff\xff",
            "start_lba": 1,
            "type": 11,
        }

        bios_parameter_block = {
            "backup_boot_sector": 6,
            "bytes_per_sector": 512,
            "data_start_sector": 2048,
            "fat_size": 1008,
            "fat_size_16": 0,
            "fat_size_32": 1008,
            "fat_start_sector": 32,
            "fs_info_sector": 1,
            "num_fats": 2,
            "reserved_sector_count": 32,
            "root_cluster": 2,
            "root_dir_first_cluster": 2,
            "sectors_per_cluster": 1,
            "total_sectors": 131070,
            "total_sectors_16": 0,
            "total_sectors_32": 131070,
        }

        assert disk.partitions == partition_table
        assert disk.partition == partition
        assert disk.bios_parameter_block == bios_parameter_block


def test_list_files(drive):
    with open(drive, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block)
        disk.init()

        files = [
            {
                "name": "DRIVE",
                "attr": 40,
                "attributes": {"V", "A"},
                "start_cluster": 0,
                "size": 0,
                "created": "1980-00-00 00:00:00.0",
                "accessed": "1980-00-00",
                "written": "2025-07-28 10:11:02",
                "is_lfn": False,
            },
            {
                "name": "A.\x00f\x00s\x00e\x00v\x00",
                "attr": 15,
                "attributes": {"R", "S", "H", "V"},
                "start_cluster": 7536640,
                "size": 4294967295,
                "created": "1980-03-14 00:03:10.218",
                "accessed": "1980-03-20",
                "written": "1980-00-00 00:03:08",
                "is_lfn": True,
            },
            {
                "name": "FSEVEN~1",
                "attr": 18,
                "attributes": {"H", "D"},
                "start_cluster": 3,
                "size": 0,
                "created": "2025-07-28 09:25:12.144",
                "accessed": "2025-07-28",
                "written": "2025-07-28 09:25:12",
                "is_lfn": False,
            },
            {
                "name": "LOG-1",
                "attr": 32,
                "attributes": {"A"},
                "start_cluster": 21,
                "size": 11,
                "created": "2025-07-14 10:42:14.0",
                "accessed": "2025-07-28",
                "written": "2025-07-16 16:32:46",
                "is_lfn": False,
            },
            {
                "name": "A.\x00_\x00L\x00O\x00G\x00",
                "attr": 15,
                "attributes": {"R", "S", "H", "V"},
                "start_cluster": 4294901760,
                "size": 4294967295,
                "created": "1980-01-17 00:01:26.234",
                "accessed": "1980-00-00",
                "written": "2107-15-31 31:63:62",
                "is_lfn": True,
            },
            {
                "name": "_LOG-~3",
                "attr": 34,
                "attributes": {"H", "A"},
                "start_cluster": 22,
                "size": 4096,
                "created": "2025-07-28 10:11:02.35",
                "accessed": "2025-07-28",
                "written": "2025-07-28 10:11:02",
                "is_lfn": False,
            },
            {
                "name": "LOG-2",
                "attr": 32,
                "attributes": {"A"},
                "start_cluster": 30,
                "size": 52117,
                "created": "2025-07-14 10:42:22.0",
                "accessed": "2025-07-28",
                "written": "2025-07-22 08:44:34",
                "is_lfn": False,
            },
            {
                "name": "A.\x00_\x00L\x00O\x00G\x00",
                "attr": 15,
                "attributes": {"R", "S", "H", "V"},
                "start_cluster": 4294901760,
                "size": 4294967295,
                "created": "1980-01-18 00:01:26.218",
                "accessed": "1980-00-00",
                "written": "2107-15-31 31:63:62",
                "is_lfn": True,
            },
            {
                "name": "_LOG-~4",
                "attr": 34,
                "attributes": {"H", "A"},
                "start_cluster": 132,
                "size": 4096,
                "created": "2025-07-28 10:11:02.41",
                "accessed": "2025-07-28",
                "written": "2025-07-28 10:11:02",
                "is_lfn": False,
            },
        ]

        assert disk.list_root_files() == files


def test_read_files(drive):
    with open(drive, "rb") as f:

        def read_block(logical_block_address):
            f.seek(logical_block_address * 512)
            return f.read(512)

        disk = Disk(read_block)
        disk.init()

        strings = []
        for file in disk.list_root_files():
            if file["size"] > 0 and file["start_cluster"] > 0 and not file["is_lfn"]:
                s = ""
                for chunk in disk.read_file_in_chunks(file):
                    s += chunk.decode("ascii", errors="replace")

                strings.append(s)

        assert [len(s) for s in strings] == [512, 4096, 52224, 4096]
