import struct
from .models import BiosParameterBlock, Partition, File

LOGICAL_BLOCK_SIZE = 512


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
        return Partition.parse_partitions(data)

    def _get_bios_parameter_block(self):
        partition_offset = self.partition.get_partition_offset()
        data = self._read_disk(partition_offset)
        return BiosParameterBlock(data)

    def _get_root_directory_entries(self):
        bytes_per_cluster = self.bios_parameter_block.get_bytes_per_cluster()

        data = bytearray()
        for chunk in self._read_file_in_chunks(
            # TODO: Figure out how to get the actual size of the directory "file"
            File(None, None, None, 2, bytes_per_cluster, None, None, None, None)
        ):
            data.extend(chunk)

        return File.parse_directory_entries(data)

    def _get_next_cluster(self, cluster):
        partition_offset = self.partition.get_partition_offset()
        offset = self.bios_parameter_block.get_fat_table_byte_offset()

        cluster_byte_start = cluster * 4
        sector_num = cluster_byte_start // 512

        data = self._read_disk(
            partition_offset + offset + (sector_num * LOGICAL_BLOCK_SIZE)
        )
        start = cluster_byte_start % LOGICAL_BLOCK_SIZE
        data = data[start : start + 4]
        return struct.unpack("<I", data)[0] & 0x0FFFFFFF

    def _read_file_in_chunks(self, file):
        cluster = file.start_cluster
        partition_offset = self.partition.get_partition_offset()
        data_sector_bytes_offset = (
            self.bios_parameter_block.get_data_sector_bytes_offset()
        )
        bytes_per_cluster = self.bios_parameter_block.get_bytes_per_cluster()

        i = 1
        while cluster < 0x0FFFFFF8:
            offset = data_sector_bytes_offset + ((cluster - 2) * bytes_per_cluster)

            sectors_to_read = self.bios_parameter_block.get_sectors_per_cluster()
            if file.size < (i * bytes_per_cluster):
                sectors_to_read = (
                    bytes_per_cluster - ((i * bytes_per_cluster) - file.size)
                ) // 512
                if file.size % LOGICAL_BLOCK_SIZE != 0:
                    sectors_to_read += 1

            next_cluster = self._get_next_cluster(cluster)
            for sec in range(0, sectors_to_read):
                data = self._read_disk(
                    partition_offset + offset + (sec * LOGICAL_BLOCK_SIZE)
                )
                if sec == sectors_to_read - 1 and next_cluster >= 0x0FFFFFF8:
                    data = data[
                        0 : file.size % LOGICAL_BLOCK_SIZE or LOGICAL_BLOCK_SIZE
                    ]
                yield data

            i += 1
            cluster = next_cluster

    def _find_next_empty_fat_entry(self, cluster):
        partition_offset = self.partition.get_partition_offset()
        fat_table_byte_offset = self.bios_parameter_block.get_fat_table_byte_offset()
        fat_sectors = self.bios_parameter_block.get_fat_size_in_sectors()

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
        partition_offset = self.partition.get_partition_offset()
        fat_table_byte_offset = self.bios_parameter_block.get_fat_table_byte_offset()

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
        partition_offset = self.partition.get_partition_offset()
        fat_table_byte_offset = self.bios_parameter_block.get_fat_table_byte_offset()

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
        cluster = file.start_cluster
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
        file.size = file.size + num_bytes_written
        return file

    def _write_to_last_cluster(self, file, data):
        partition_offset = self.partition.get_partition_offset()
        bytes_per_cluster = self.bios_parameter_block.get_bytes_per_cluster()
        data_sector_bytes_offset = (
            self.bios_parameter_block.get_data_sector_bytes_offset()
        )
        file_size = file.size
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
            file.size = file.size + written
            if len(data) > 0:
                last_cluster = self._get_files_last_cluster(file)
                self._allocate_next_free_cluster(last_cluster)

        return file

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

    @classmethod
    def _get_largest_non_empty_partition(cls, partitions):
        return sorted(partitions, key=lambda x: (x.num_sectors * -1, x.start_lba))[0]
