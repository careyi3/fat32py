import struct

LOGICAL_BLOCK_SIZE = 512
PARTITION_BLOCK_SIZE = 512


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

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.partitions = []
        self.partition = None
        self.bios_parameter_block = None
        self.initialised = False

    # Private Instance Methods
    def _read_disk(self, offset):
        block = offset // LOGICAL_BLOCK_SIZE
        return bytearray(self.reader(block))

    def _write_disk(self, offset, data):
        block = offset // LOGICAL_BLOCK_SIZE
        return self.writer(block, data)

    def _get_partitions(self):
        data = self._read_disk(0)
        return self._parse_partitions(data)

    def _get_partition_offset(self):
        partition_sector_offset = self.partition["start_lba"]
        partition_offset = partition_sector_offset * PARTITION_BLOCK_SIZE
        return partition_offset

    def _get_bios_parameter_block(self):
        partition_offset = self._get_partition_offset()
        data = self._read_disk(partition_offset)
        return self._parse_bios_parameter_block(data)

    def _get_bytes_per_cluster(self):
        bytes_per_sector = self.bios_parameter_block["bytes_per_sector"]
        sectors_per_cluster = self.bios_parameter_block["sectors_per_cluster"]
        return sectors_per_cluster * bytes_per_sector

    def _get_sectors_per_cluster(self):
        sectors_per_cluster = self.bios_parameter_block["sectors_per_cluster"]
        return sectors_per_cluster

    def _get_root_directory_entries(self):
        bytes_per_cluster = self._get_bytes_per_cluster()

        data = bytearray()
        for chunk in self._read_file_in_chunks(
            {"size": bytes_per_cluster, "start_cluster": 2},
        ):
            data.extend(chunk)

        return self._parse_directory_entries(data)

    def _get_fat_table_byte_offset(self):
        fat_start_sector = self.bios_parameter_block["fat_start_sector"]
        bytes_per_sector = self.bios_parameter_block["bytes_per_sector"]
        return fat_start_sector * bytes_per_sector

    def _get_next_cluster(self, cluster):
        partition_offset = self._get_partition_offset()
        offset = self._get_fat_table_byte_offset()

        cluster_byte_start = cluster * 4
        sector_num = cluster_byte_start // 512

        data = self._read_disk(
            partition_offset + offset + (sector_num * LOGICAL_BLOCK_SIZE)
        )
        start = cluster_byte_start % LOGICAL_BLOCK_SIZE
        data = data[start : start + 4]
        return struct.unpack("<I", data)[0] & 0x0FFFFFFF

    def _get_data_sector_bytes_offset(self):
        bytes_per_sector = self.bios_parameter_block["bytes_per_sector"]
        data_sector_starts = self.bios_parameter_block["data_start_sector"]
        return data_sector_starts * bytes_per_sector

    def _read_file_in_chunks(self, file):
        cluster = file["start_cluster"]
        partition_offset = self._get_partition_offset()
        data_sector_bytes_offset = self._get_data_sector_bytes_offset()
        bytes_per_cluster = self._get_bytes_per_cluster()

        i = 1
        while cluster < 0x0FFFFFF8:
            offset = data_sector_bytes_offset + ((cluster - 2) * bytes_per_cluster)

            sectors_to_read = self._get_sectors_per_cluster()
            if file["size"] < (i * bytes_per_cluster):
                sectors_to_read = (
                    bytes_per_cluster - ((i * bytes_per_cluster) - file["size"])
                ) // 512
                if file["size"] % LOGICAL_BLOCK_SIZE != 0:
                    sectors_to_read += 1

            next_cluster = self._get_next_cluster(cluster)
            for sec in range(0, sectors_to_read):
                data = self._read_disk(
                    partition_offset + offset + (sec * LOGICAL_BLOCK_SIZE)
                )
                if sec == sectors_to_read - 1 and next_cluster >= 0x0FFFFFF8:
                    data = data[
                        0 : file["size"] % LOGICAL_BLOCK_SIZE or LOGICAL_BLOCK_SIZE
                    ]
                yield data

            i += 1
            cluster = next_cluster

    def _get_fat_size_in_sectors(self):
        return self.bios_parameter_block["fat_size_32"]

    def _find_next_empty_fat_entry(self, cluster):
        partition_offset = self._get_partition_offset()
        fat_table_byte_offset = self._get_fat_table_byte_offset()
        fat_sectors = self._get_fat_size_in_sectors()

        idx = None
        start_sector = (cluster * 4) // LOGICAL_BLOCK_SIZE
        sector_num = start_sector
        data = None
        start_id = (cluster * 4) % LOGICAL_BLOCK_SIZE
        while idx == None:
            data = self._read_disk(
                partition_offset
                + fat_table_byte_offset
                + (sector_num * LOGICAL_BLOCK_SIZE)
            )
            for i in range(start_id, len(data), 4):
                content = struct.unpack("<I", data[i : i + 4])[0] & 0x0FFFFFFF
                if content == 0x00000000:
                    idx = i
                    break
            start_id = 0
            sector_num += 1
            if sector_num == fat_sectors:
                sector_num = 0
            if start_sector == sector_num:
                break

        return (data, sector_num, idx)

    def _write_fat_entry(self, data, sector_num, idx, entry):
        partition_offset = self._get_partition_offset()
        fat_table_byte_offset = self._get_fat_table_byte_offset()

        data[idx] = entry[0]
        data[idx + 1] = entry[1]
        data[idx + 2] = entry[2]
        data[idx + 3] = entry[3]

        self._write_disk(
            partition_offset
            + fat_table_byte_offset
            + (sector_num * LOGICAL_BLOCK_SIZE),
            data,
        )

    def _get_fat_block_for_cluster(self, cluster):
        partition_offset = self._get_partition_offset()
        fat_table_byte_offset = self._get_fat_table_byte_offset()

        idx = (cluster * 4) % LOGICAL_BLOCK_SIZE
        sector_num = (cluster * 4) // LOGICAL_BLOCK_SIZE
        data = self._read_disk(
            partition_offset + fat_table_byte_offset + (sector_num * LOGICAL_BLOCK_SIZE)
        )

        return (data, sector_num, idx)

    def _allocate_next_free_cluster(self, last_cluster):
        data, sector_num, idx = self._find_next_empty_fat_entry(last_cluster)
        entry = (0x0FFFFFF8 & 0x0FFFFFFF).to_bytes(4, byteorder="little")
        self._write_fat_entry(data, sector_num, idx, entry)
        new_cluster = ((sector_num * LOGICAL_BLOCK_SIZE) + idx) // 4

        data, sector_num, idx = self._get_fat_block_for_cluster(last_cluster)

        entry = (new_cluster & 0x0FFFFFFF).to_bytes(4, byteorder="little")
        self._write_fat_entry(data, sector_num, idx, entry)

    def _get_files_last_cluster(self, file):
        cluster = file["start_cluster"]
        while cluster < 0x0FFFFFF8:
            next_cluster = self._get_next_cluster(cluster)
            if next_cluster < 0x0FFFFFF8:
                cluster = self._get_next_cluster(cluster)
            else:
                return cluster
        return cluster

    def _create_file_record(self, file):
        # TODO: Make this actually create the file record
        return file

    def _update_file_record(self, file, num_bytes_written):
        # TODO: Make this actually update the file record
        file["size"] = file["size"] + num_bytes_written
        return file

    def _write_to_last_cluster(self, file, data):
        partition_offset = self._get_partition_offset()
        bytes_per_cluster = self._get_bytes_per_cluster()
        data_sector_bytes_offset = self._get_data_sector_bytes_offset()
        file_size = file["size"]
        free_bytes_in_cluster = bytes_per_cluster - (file_size % bytes_per_cluster)
        last_cluster = self._get_files_last_cluster(file)

        cluster_used_bytes = file_size % bytes_per_cluster
        used_sectors = cluster_used_bytes // LOGICAL_BLOCK_SIZE
        eof_index = cluster_used_bytes % LOGICAL_BLOCK_SIZE
        offset = data_sector_bytes_offset + ((last_cluster - 2) * bytes_per_cluster)
        block = self._read_disk(
            partition_offset + offset + (used_sectors * LOGICAL_BLOCK_SIZE)
        )

        written = 0
        while free_bytes_in_cluster > 0:
            if len(data) == 0:
                break

            d = data.pop(0)
            written += 1
            free_bytes_in_cluster -= 1
            block[eof_index] = d
            eof_index += 1
            if eof_index == LOGICAL_BLOCK_SIZE:
                if len(data) > 0:
                    self._write_disk(
                        partition_offset + offset + (used_sectors * LOGICAL_BLOCK_SIZE),
                        block,
                    )
                    used_sectors += 1
                    eof_index = 0
                    block = self._read_disk(
                        partition_offset + offset + (used_sectors * LOGICAL_BLOCK_SIZE)
                    )

        self._write_disk(
            partition_offset + offset + (used_sectors * LOGICAL_BLOCK_SIZE),
            block,
        )

        return data, written

    def _append_to_file(self, file, data):
        while len(data) > 0:
            data, written = self._write_to_last_cluster(file, data)
            file = self._update_file_record(file, written)
            if len(data) > 0:
                last_cluster = self._get_files_last_cluster(file)
                self._allocate_next_free_cluster(last_cluster)

        return file

    # Private Class Methods
    @classmethod
    def _parse_partitions(cls, data):
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

    @classmethod
    def _get_largest_non_empty_partition(cls, partitions):
        return sorted(
            partitions, key=lambda x: (x["num_sectors"] * -1, x["start_lba"])
        )[0]

    @classmethod
    def _parse_bios_parameter_block(cls, data):
        bios_parameter_block = {}
        bios_parameter_block["bytes_per_sector"] = struct.unpack_from("<H", data, 11)[0]
        bios_parameter_block["sectors_per_cluster"] = data[13]
        bios_parameter_block["reserved_sector_count"] = struct.unpack_from(
            "<H", data, 14
        )[0]
        bios_parameter_block["num_fats"] = data[16]
        bios_parameter_block["total_sectors_16"] = struct.unpack_from("<H", data, 19)[0]
        bios_parameter_block["total_sectors_32"] = struct.unpack_from("<I", data, 32)[0]
        bios_parameter_block["fat_size_16"] = struct.unpack_from("<H", data, 22)[0]
        bios_parameter_block["fat_size_32"] = struct.unpack_from("<I", data, 36)[0]
        bios_parameter_block["root_cluster"] = struct.unpack_from("<I", data, 44)[0]
        bios_parameter_block["fs_info_sector"] = struct.unpack_from("<H", data, 48)[0]
        bios_parameter_block["backup_boot_sector"] = struct.unpack_from("<H", data, 50)[
            0
        ]

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

    @classmethod
    def _parse_directory_entries(cls, data):
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
                    "attributes": cls._attributes(attr),
                    "start_cluster": start_cluster,
                    "size": file_size,
                    "created": f"{decode_date(crt_date)} {decode_time(crt_time)}.{crt_time_tenth}",
                    "accessed": decode_date(lst_acc_date),
                    "written": f"{decode_date(wrt_date)} {decode_time(wrt_time)}",
                    "is_lfn": is_lfn_entry(entry),
                }
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

    # Public Instance Methods
    def init(self):
        self.partitions = self._get_partitions()
        self.partition = self._get_largest_non_empty_partition(self.partitions)
        self.bios_parameter_block = self._get_bios_parameter_block()
        self.initialised = True

    def list_root_files(self):
        if not self.initialised:
            raise DiskNotInitialised
        return self._get_root_directory_entries()

    def read_file_in_chunks(self, file):
        if not self.initialised:
            raise DiskNotInitialised
        yield from self._read_file_in_chunks(file)

    def append_to_file(self, file, data):
        if not self.initialised:
            raise DiskNotInitialised
        return self._append_to_file(file, data)

    # Public Class Methods
