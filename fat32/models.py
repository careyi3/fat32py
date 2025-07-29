import struct

PARTITION_BLOCK_SIZE = 512


class BiosParameterBlock:
    def __init__(self, data):
        self.bytes_per_sector = struct.unpack_from("<H", data, 11)[0]
        self.sectors_per_cluster = data[13]
        self.reserved_sector_count = struct.unpack_from("<H", data, 14)[0]
        self.num_fats = data[16]
        self.total_sectors_16 = struct.unpack_from("<H", data, 19)[0]
        self.total_sectors_32 = struct.unpack_from("<I", data, 32)[0]
        self.fat_size_16 = struct.unpack_from("<H", data, 22)[0]
        self.fat_size_32 = struct.unpack_from("<I", data, 36)[0]
        self.root_cluster = struct.unpack_from("<I", data, 44)[0]
        self.fs_info_sector = struct.unpack_from("<H", data, 48)[0]
        self.backup_boot_sector = struct.unpack_from("<H", data, 50)[0]

        self.fat_size = self.fat_size_32 if self.fat_size_16 == 0 else self.fat_size_16
        self.total_sectors = (
            self.total_sectors_32
            if self.total_sectors_16 == 0
            else self.total_sectors_16
        )
        self.fat_start_sector = self.reserved_sector_count
        self.data_start_sector = self.reserved_sector_count + (
            self.num_fats * self.fat_size
        )
        self.root_dir_first_cluster = self.root_cluster

    def to_dict(self):
        return {
            "bytes_per_sector": self.bytes_per_sector,
            "sectors_per_cluster": self.sectors_per_cluster,
            "reserved_sector_count": self.reserved_sector_count,
            "num_fats": self.num_fats,
            "total_sectors_16": self.total_sectors_16,
            "total_sectors_32": self.total_sectors_32,
            "fat_size_16": self.fat_size_16,
            "fat_size_32": self.fat_size_32,
            "root_cluster": self.root_cluster,
            "fs_info_sector": self.fs_info_sector,
            "backup_boot_sector": self.backup_boot_sector,
            "fat_size": self.fat_size,
            "total_sectors": self.total_sectors,
            "fat_start_sector": self.fat_start_sector,
            "data_start_sector": self.data_start_sector,
            "root_dir_first_cluster": self.root_dir_first_cluster,
        }

    def get_bytes_per_cluster(self):
        bytes_per_sector = self.bytes_per_sector
        sectors_per_cluster = self.sectors_per_cluster
        return sectors_per_cluster * bytes_per_sector

    def get_sectors_per_cluster(self):
        sectors_per_cluster = self.sectors_per_cluster
        return sectors_per_cluster


class Partition:
    def __init__(
        self, boot_flag, start_chs, part_type, end_chs, start_lba, num_sectors
    ):
        self.boot_flag = boot_flag
        self.start_chs = start_chs
        self.type = part_type
        self.end_chs = end_chs
        self.start_lba = start_lba
        self.num_sectors = num_sectors

    def to_dict(self):
        return {
            "boot_flag": self.boot_flag,
            "start_chs": self.start_chs,
            "type": self.type,
            "end_chs": self.end_chs,
            "start_lba": self.start_lba,
            "num_sectors": self.num_sectors,
        }

    def get_partition_offset(self):
        partition_sector_offset = self.start_lba
        partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE
        return partition_offset

    @classmethod
    def parse_partitions(cls, data):
        parsed_entries = []
        for i in range(4):
            start_idx = 446 + (i * 16)
            end_idx = start_idx + 16
            entry = data[start_idx:end_idx]

            boot_flag = entry[0]
            start_chs = entry[1:4]
            part_type = entry[4]
            end_chs = entry[5:8]
            start_lba = struct.unpack("<I", entry[8:12])[0]
            num_sectors = struct.unpack("<I", entry[12:16])[0]

            parsed_entries.append(
                cls(
                    boot_flag=boot_flag,
                    start_chs=start_chs,
                    part_type=part_type,
                    end_chs=end_chs,
                    start_lba=start_lba,
                    num_sectors=num_sectors,
                )
            )
        return parsed_entries

    @classmethod
    def get_largest_non_empty_partition(cls, partitions):
        return sorted(partitions, key=lambda x: (x.num_sectors * -1, x.start_lba))[0]
