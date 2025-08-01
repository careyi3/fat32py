import struct
from typing import Any, Dict, List, Optional, Set, Union

PARTITION_BLOCK_SIZE: int = 512


class BiosParameterBlock:
    """
    Represents the BIOS Parameter Block for a FAT32 partition.

    Attributes:
        bytes_per_sector (int): Number of bytes per sector.
        sectors_per_cluster (int): Number of sectors per cluster.
        reserved_sector_count (int): Number of reserved sectors.
        num_fats (int): Number of FAT tables.
        total_sectors_16 (int): Total sectors (16-bit).
        total_sectors_32 (int): Total sectors (32-bit).
        fat_size_16 (int): FAT size in sectors (16-bit).
        fat_size_32 (int): FAT size in sectors (32-bit).
        root_cluster (int): First cluster of the root directory.
        fs_info_sector (int): FSInfo sector number.
        backup_boot_sector (int): Backup boot sector number.
        fat_size (int): FAT size in sectors.
        total_sectors (int): Total sectors.
        fat_start_sector (int): FAT start sector.
        data_start_sector (int): Data start sector.
        root_dir_first_cluster (int): First cluster of the root directory.
    """

    def __init__(self, data: bytes) -> None:
        """
        Initialize a BiosParameterBlock from raw sector data.

        Parameters:
            data (bytes): Raw sector data.
        Returns:
            None
        """
        self.bytes_per_sector: int = struct.unpack_from("<H", data, 11)[0]
        self.sectors_per_cluster: int = data[13]
        self.reserved_sector_count: int = struct.unpack_from("<H", data, 14)[0]
        self.num_fats: int = data[16]
        self.total_sectors_16: int = struct.unpack_from("<H", data, 19)[0]
        self.total_sectors_32: int = struct.unpack_from("<I", data, 32)[0]
        self.fat_size_16: int = struct.unpack_from("<H", data, 22)[0]
        self.fat_size_32: int = struct.unpack_from("<I", data, 36)[0]
        self.root_cluster: int = struct.unpack_from("<I", data, 44)[0]
        self.fs_info_sector: int = struct.unpack_from("<H", data, 48)[0]
        self.backup_boot_sector: int = struct.unpack_from("<H", data, 50)[0]

        self.fat_size: int = (
            self.fat_size_32 if self.fat_size_16 == 0 else self.fat_size_16
        )
        self.total_sectors: int = (
            self.total_sectors_32
            if self.total_sectors_16 == 0
            else self.total_sectors_16
        )
        self.fat_start_sector: int = self.reserved_sector_count
        self.data_start_sector: int = self.reserved_sector_count + (
            self.num_fats * self.fat_size
        )
        self.root_dir_first_cluster: int = self.root_cluster

    def to_dict(self) -> Dict[str, int]:
        """
        Serialize the BIOS Parameter Block to a dictionary.

        Returns:
            Dict[str, int]: Dictionary of BPB fields.
        """
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

    def get_bytes_per_cluster(self) -> int:
        """
        Get the number of bytes per cluster.

        Returns:
            int: Bytes per cluster.
        """
        return self.sectors_per_cluster * self.bytes_per_sector

    def get_sectors_per_cluster(self) -> int:
        """
        Get the number of sectors per cluster.

        Returns:
            int: Sectors per cluster.
        """
        return self.sectors_per_cluster

    def get_fat_table_byte_offset(self) -> int:
        """
        Get the byte offset of the FAT table.

        Returns:
            int: FAT table byte offset.
        """
        return self.fat_start_sector * self.bytes_per_sector

    def get_data_sector_bytes_offset(self) -> int:
        """
        Get the byte offset of the data sector.

        Returns:
            int: Data sector byte offset.
        """
        return self.data_start_sector * self.bytes_per_sector

    def get_fat_size_in_sectors(self) -> int:
        """
        Get the FAT size in sectors.

        Returns:
            int: FAT size in sectors.
        """
        return self.fat_size_32

    def get_root_dir_first_cluster(self) -> int:
        """
        Get the cluster id for the root directory

        Returns:
            int: Cluster number
        """
        return self.root_dir_first_cluster


class Partition:
    """
    Represents a partition entry in the partition table.

    Attributes:
        boot_flag (int): Boot flag byte.
        start_chs (bytes): Starting CHS address.
        type (int): Partition type byte.
        end_chs (bytes): Ending CHS address.
        start_lba (int): Starting LBA sector.
        num_sectors (int): Number of sectors in the partition.
    """

    def __init__(
        self,
        boot_flag: int,
        start_chs: bytes,
        part_type: int,
        end_chs: bytes,
        start_lba: int,
        num_sectors: int,
    ) -> None:
        """
        Initialize a Partition object.

        Parameters:
            boot_flag (int): Boot flag byte.
            start_chs (bytes): Starting CHS address.
            part_type (int): Partition type byte.
            end_chs (bytes): Ending CHS address.
            start_lba (int): Starting LBA sector.
            num_sectors (int): Number of sectors in the partition.
        Returns:
            None
        """
        self.boot_flag = boot_flag
        self.start_chs = start_chs
        self.type = part_type
        self.end_chs = end_chs
        self.start_lba = start_lba
        self.num_sectors = num_sectors

    def to_dict(self) -> Dict[str, Union[int, bytes]]:
        """
        Serialize the Partition to a dictionary.

        Returns:
            Dict[str, Union[int, bytes]]: Dictionary of partition fields.
        """
        return {
            "boot_flag": self.boot_flag,
            "start_chs": self.start_chs,
            "type": self.type,
            "end_chs": self.end_chs,
            "start_lba": self.start_lba,
            "num_sectors": self.num_sectors,
        }

    def get_partition_offset(self) -> int:
        """
        Get the byte offset of the partition start.

        Returns:
            int: Partition byte offset.
        """
        partition_sector_offset = self.start_lba
        partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE
        return partition_offset

    @classmethod
    def parse_partitions(cls, data: bytes) -> List["Partition"]:
        """
        Parse up to 4 partition entries from raw MBR data.

        Parameters:
            data (bytes): Raw MBR sector data.
        Returns:
            List[Partition]: List of Partition objects.
        """
        parsed_entries: List[Partition] = []
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
    """
    Represents a file or directory entry in FAT32.

    Attributes:
        name (str): File or directory name.
        attr (int): Attribute byte.
        attributes (Set[str]): Set of attribute flags.
        start_cluster (int): Starting cluster number.
        size (int): File size in bytes.
        created (str): Creation timestamp.
        accessed (str): Last access date.
        written (str): Last write timestamp.
        is_lfn (bool): Whether entry is a long file name.
        byte_offset (int): Start position of the file record in bytes from the start of the data sector.
    """

    def __init__(
        self,
        name: str,
        attr: int,
        attributes: Set[str],
        start_cluster: int,
        size: int,
        created: str,
        accessed: str,
        written: str,
        is_lfn: bool,
        byte_offset: int,
    ) -> None:
        """
        Initialize a File object.

        Parameters:
            name (str): File or directory name.
            attr (int): Attribute byte.
            attributes (Set[str]): Set of attribute flags.
            start_cluster (int): Starting cluster number.
            size (int): File size in bytes.
            created (str): Creation timestamp.
            accessed (str): Last access date.
            written (str): Last write timestamp.
            is_lfn (bool): Whether entry is a long file name.
        Returns:
            None
        """
        self.name = name
        self.attr = attr
        self.attributes = attributes
        self.start_cluster = start_cluster
        self.size = size
        self.created = created
        self.accessed = accessed
        self.written = written
        self.is_lfn = is_lfn
        self.byte_offset = byte_offset

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the File to a dictionary.

        Returns:
            Dict[str, Any]: Dictionary of file fields.
        """
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
            "byte_offset": self.byte_offset,
        }

    def to_bytes(self) -> bytes:
        """
        Serialize the File object to a 32-byte FAT32 directory entry.

        Returns:
            bytes: The raw 32-byte directory entry.
        """
        name_bytes = self.name.encode("ascii", errors="replace")[:11]
        name_bytes = name_bytes.ljust(11, b" ")
        attr_byte = self.attr
        nt_byte = 0
        crt_time_tenth = 0
        try:
            date_part, time_part = self.created.split(" ")
            year, month, day = [int(x) for x in date_part.split("-")]
            hour, minute, second = [int(x) for x in time_part.split(":")[:3]]
            crt_date = ((year - 1980) << 9) | (month << 5) | day
            crt_time = (hour << 11) | (minute << 5) | (second // 2)
        except Exception:
            crt_date = 0
            crt_time = 0
        try:
            year, month, day = [int(x) for x in self.accessed.split("-")]
            lst_acc_date = ((year - 1980) << 9) | (month << 5) | day
        except Exception:
            lst_acc_date = 0
        fst_clus_hi = (self.start_cluster >> 16) & 0xFFFF
        try:
            date_part, time_part = self.written.split(" ")
            year, month, day = [int(x) for x in date_part.split("-")]
            hour, minute, second = [int(x) for x in time_part.split(":")[:3]]
            wrt_date = ((year - 1980) << 9) | (month << 5) | day
            wrt_time = (hour << 11) | (minute << 5) | (second // 2)
        except Exception:
            wrt_date = 0
            wrt_time = 0
        fst_clus_lo = self.start_cluster & 0xFFFF
        file_size = self.size
        entry = struct.pack(
            "<11sBBBHHHHHHHI",
            name_bytes,
            attr_byte,
            nt_byte,
            crt_time_tenth,
            crt_time,
            crt_date,
            lst_acc_date,
            fst_clus_hi,
            wrt_time,
            wrt_date,
            fst_clus_lo,
            file_size,
        )
        return entry

    @classmethod
    def parse_directory_entries(cls, data: bytes, sector_offset: int) -> List["File"]:
        """
        Parse directory entries from raw directory data.

        Parameters:
            data (bytes): Raw directory data.
            sector_offset (int): Sector offset in bytes from start of data sector
        Returns:
            List[File]: List of File objects.
        """
        entries: List[File] = []
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

            def decode_date(d: int) -> str:
                year = ((d >> 9) & 0x7F) + 1980
                month = (d >> 5) & 0x0F
                day = d & 0x1F
                return f"{year:04}-{month:02}-{day:02}"

            def decode_time(t: int) -> str:
                hour = (t >> 11) & 0x1F
                minute = (t >> 5) & 0x3F
                second = (t & 0x1F) * 2
                return f"{hour:02}:{minute:02}:{second:02}"

            def is_lfn_entry(entry: bytes) -> bool:
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
                    created=f"{decode_date(crt_date)} {decode_time(crt_time)}",
                    accessed=decode_date(lst_acc_date),
                    written=f"{decode_date(wrt_date)} {decode_time(wrt_time)}",
                    is_lfn=is_lfn_entry(entry),
                    byte_offset=sector_offset + i,
                )
            )
        return entries

    @classmethod
    def _attributes(cls, attr: int) -> Set[str]:
        """
        Decode the attribute byte into a set of attribute flags.

        Parameters:
            attr (int): Attribute byte.
        Returns:
            Set[str]: Set of attribute flags.
        """
        flags: Set[str] = set()
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
