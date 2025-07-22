import struct

LOGICAL_BLOCK_SIZE = 512
PARTITION_BLOCK_SIZE = 512


def _read_disk(reader, offset):
    block = offset // LOGICAL_BLOCK_SIZE
    return reader(block)


def _parse_partitions(data):
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
            {
                "boot_flag": boot_flag,
                "start_chs": start_chs,
                "type": part_type,
                "end_chs": end_chs,
                "start_lba": start_lba,
                "num_sectors": num_sectors,
            }
        )
    return parsed_entries


def _get_partitions(reader):
    data = _read_disk(reader, 0)
    return _parse_partitions(data)


def _get_largest_non_empty_partition(partitions):
    return sorted(partitions, key=lambda x: (x["num_sectors"] * -1, x["start_lba"]))[0]


def _parse_bios_parameter_block(data):
    bios_parameter_block = {}
    bios_parameter_block["bytes_per_sector"] = struct.unpack_from("<H", data, 11)[0]
    bios_parameter_block["sectors_per_cluster"] = data[13]
    bios_parameter_block["reserved_sector_count"] = struct.unpack_from("<H", data, 14)[
        0
    ]
    bios_parameter_block["num_fats"] = data[16]
    bios_parameter_block["total_sectors_16"] = struct.unpack_from("<H", data, 19)[0]
    bios_parameter_block["total_sectors_32"] = struct.unpack_from("<I", data, 32)[0]
    bios_parameter_block["fat_size_16"] = struct.unpack_from("<H", data, 22)[0]
    bios_parameter_block["fat_size_32"] = struct.unpack_from("<I", data, 36)[0]
    bios_parameter_block["root_cluster"] = struct.unpack_from("<I", data, 44)[0]
    bios_parameter_block["fs_info_sector"] = struct.unpack_from("<H", data, 48)[0]
    bios_parameter_block["backup_boot_sector"] = struct.unpack_from("<H", data, 50)[0]

    bios_parameter_block["fat_size"] = (
        bios_parameter_block["fat_size_32"]
        if bios_parameter_block["fat_size_16"] == 0
        else bios_parameter_block["fat_size_16"]
    )
    bios_parameter_block["total_sectors"] = (
        bios_parameter_block["total_sectors_32"]
        if bios_parameter_block["total_sectors_16"] == 0
        else bios_parameter_block["total_sectors_16"]
    )

    bios_parameter_block["fat_start_sector"] = bios_parameter_block[
        "reserved_sector_count"
    ]
    bios_parameter_block["data_start_sector"] = bios_parameter_block[
        "reserved_sector_count"
    ] + (bios_parameter_block["num_fats"] * bios_parameter_block["fat_size"])
    bios_parameter_block["root_dir_first_cluster"] = bios_parameter_block[
        "root_cluster"
    ]

    return bios_parameter_block


def _get_bios_parameter_block(reader, partition):
    partition_sector_offset = partition["start_lba"]
    partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE
    data = _read_disk(reader, partition_offset)
    return _parse_bios_parameter_block(data)


def _parse_directory_entries(data):
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
            {
                "name": name,
                "attr": attr,
                "attributes": _attributes(attr),
                "start_cluster": start_cluster,
                "size": file_size,
                "created": f"{decode_date(crt_date)} {decode_time(crt_time)}.{crt_time_tenth}",
                "accessed": decode_date(lst_acc_date),
                "written": f"{decode_date(wrt_date)} {decode_time(wrt_time)}",
                "is_lfn": is_lfn_entry(entry),
            }
        )
    return entries


def _attributes(attr):
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


def _get_root_directory_entries(reader, partition, bios_parameter_block):
    bytes_per_sector = bios_parameter_block["bytes_per_sector"]
    sectors_per_cluster = bios_parameter_block["sectors_per_cluster"]
    bytes_per_cluster = sectors_per_cluster * bytes_per_sector

    data = bytearray()
    for chunk in _read_file_in_chunks(
        reader,
        partition,
        bios_parameter_block,
        {"size": bytes_per_cluster, "start_cluster": 2},
    ):
        data.extend(chunk)

    return _parse_directory_entries(data)


def _get_next_cluster(reader, partition, bios_parameter_block, cluster):
    partition_sector_offset = partition["start_lba"]
    partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE

    fat_start_sector = bios_parameter_block["fat_start_sector"]
    bytes_per_sector = bios_parameter_block["bytes_per_sector"]
    offset = fat_start_sector * bytes_per_sector

    cluster_byte_start = cluster * 4
    sector_num = cluster_byte_start // 512

    data = _read_disk(
        reader, partition_offset + offset + (sector_num * LOGICAL_BLOCK_SIZE)
    )
    start = cluster_byte_start % LOGICAL_BLOCK_SIZE
    data = data[start : start + 4]
    return struct.unpack("<I", data)[0] & 0x0FFFFFFF


def _read_file(reader, partition, bios_parameter_block, file):
    content = bytearray()
    for chunk in _read_file_in_chunks(reader, partition, bios_parameter_block, file):
        content.extend(chunk)

    return content


def _read_file_in_chunks(reader, partition, bios_parameter_block, file):
    cluster = file["start_cluster"]
    partition_sector_offset = partition["start_lba"]
    partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE

    bytes_per_sector = bios_parameter_block["bytes_per_sector"]
    sectors_per_cluster = bios_parameter_block["sectors_per_cluster"]
    data_sector_starts = bios_parameter_block["data_start_sector"]

    bytes_per_cluster = sectors_per_cluster * bytes_per_sector
    i = 1
    while cluster < 0x0FFFFFF8:
        offset = (data_sector_starts * bytes_per_sector) + (
            (cluster - 2) * bytes_per_cluster
        )

        sectors_to_read = 64
        if file["size"] < (i * bytes_per_cluster):
            sectors_to_read = (
                bytes_per_cluster - ((i * bytes_per_cluster) - file["size"])
            ) // 512
            if file["size"] % LOGICAL_BLOCK_SIZE != 0:
                sectors_to_read += 1

        for sec in range(0, sectors_to_read):
            yield _read_disk(
                reader, partition_offset + offset + (sec * LOGICAL_BLOCK_SIZE)
            )

        i += 1
        cluster = _get_next_cluster(reader, partition, bios_parameter_block, cluster)


class DiskNotInitialised(Exception):
    """Exception raised when the disk isn't initialised.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class Disk:
    """Object for intefacing with a FAT32 disk.

    Attributes:
        reader -- function that can read 512 bytes at a time from a given drive
    """

    def __init__(self, reader):
        self.reader = reader
        self.partitions = []
        self.partition = None
        self.bios_parameter_block = None
        self.initialised = False

    def init(self):
        self.partitions = _get_partitions(self.reader)
        self.partition = _get_largest_non_empty_partition(self.partitions)
        self.bios_parameter_block = _get_bios_parameter_block(
            self.reader, self.partition
        )
        self.initialised = True

    def list_root_files(self):
        if not self.initialised:
            raise DiskNotInitialised
        return _get_root_directory_entries(
            self.reader, self.partition, self.bios_parameter_block
        )

    def read_file(self, file):
        if not self.initialised:
            raise DiskNotInitialised
        return _read_file(self.reader, self.partition, self.bios_parameter_block, file)

    def read_file_in_chunks(self, file):
        if not self.initialised:
            raise DiskNotInitialised
        yield from _read_file_in_chunks(
            self.reader, self.partition, self.bios_parameter_block, file
        )
