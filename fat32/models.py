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

    def get_fat_table_byte_offset(self):
        fat_start_sector = self.fat_start_sector
        bytes_per_sector = self.bytes_per_sector
        return fat_start_sector * bytes_per_sector

    def get_data_sector_bytes_offset(self):
        bytes_per_sector = self.bytes_per_sector
        data_sector_starts = self.data_start_sector
        return data_sector_starts * bytes_per_sector

    def get_fat_size_in_sectors(self):
        fat_size_in_sectors = self.fat_size_32
        return fat_size_in_sectors


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


class File:
    def __init__(
        self,
        name,
        attr,
        attributes,
        start_cluster,
        size,
        created,
        accessed,
        written,
        is_lfn,
    ):
        self.name = name
        self.attr = attr
        self.attributes = attributes
        self.start_cluster = start_cluster
        self.size = size
        self.created = created
        self.accessed = accessed
        self.written = written
        self.is_lfn = is_lfn

    def to_dict(self):
        return {
            "name": self.name,
            "attr": self.attr,
            "attributes": self.attributes,
            "start_cluster": self.start_cluster,
            "size": self.size,
            "created": self.created,
            "accessed": self.accessed,
            "written": self.written,
            "is_lfn": self.is_lfn,
        }

    @classmethod
    def parse_directory_entries(cls, data):
        entries = []
        for i in range(0, len(data), 32):
            entry = data[i : i + 32]
            if entry[0] == 0x00:
                break  # no more entries
            if entry[0] == 0xE5:
                continue  # deleted

            name = entry[0:11].decode("ascii", errors="replace").strip()
            attr = entry[11]
            crt_time_tenth = entry[13]
            crt_time = struct.unpack("<H", entry[14:16])[0]
            crt_date = struct.unpack("<H", entry[16:18])[0]
            lst_acc_date = struct.unpack("<H", entry[18:20])[0]
            fst_clus_hi = struct.unpack("<H", entry[20:22])[0]
            wrt_time = struct.unpack("<H", entry[22:24])[0]
            wrt_date = struct.unpack("<H", entry[24:26])[0]
            fst_clus_lo = struct.unpack("<H", entry[26:28])[0]
            file_size = struct.unpack("<I", entry[28:32])[0]

            start_cluster = (fst_clus_hi << 16) | fst_clus_lo

            def decode_date(d):
                year = ((d >> 9) & 0x7F) + 1980
                month = (d >> 5) & 0x0F
                day = d & 0x1F
                return f"{year:04}-{month:02}-{day:02}"

            def decode_time(t):
                hour = (t >> 11) & 0x1F
                minute = (t >> 5) & 0x3F
                second = (t & 0x1F) * 2
                return f"{hour:02}:{minute:02}:{second:02}"

            def is_lfn_entry(entry):
                attr = entry[0x0B]
                first_byte = entry[0x00]
                return attr == 0x0F and first_byte != 0x00 and first_byte != 0xE5

            entries.append(
                cls(
                    name=name,
                    attr=attr,
                    attributes=File._attributes(attr),
                    start_cluster=start_cluster,
                    size=file_size,
                    created=f"{decode_date(crt_date)} {decode_time(crt_time)}.{crt_time_tenth}",
                    accessed=decode_date(lst_acc_date),
                    written=f"{decode_date(wrt_date)} {decode_time(wrt_time)}",
                    is_lfn=is_lfn_entry(entry),
                )
            )
        return entries

    @classmethod
    def _attributes(cls, attr):
        flags = set()
        if attr & 0x01:
            flags.add("R")  # Read Only
        if attr & 0x02:
            flags.add("H")  # Hidden
        if attr & 0x04:
            flags.add("S")  # System
        if attr & 0x08:
            flags.add("V")  # Volume Label
        if attr & 0x10:
            flags.add("D")  # Directory
        if attr & 0x20:
            flags.add("A")  # Archive
        if attr & 0x40:
            flags.add("DV")  # Device
        if attr & 0x80:
            flags.add("RS")  # Reserved
        return flags
