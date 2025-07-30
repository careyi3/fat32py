import struct
from typing import Callable, List, Optional, Tuple, Generator, Any
from .models import BiosParameterBlock, Partition, File

LOGICAL_BLOCK_SIZE: int = 512


class DiskNotInitialised(Exception):
    """Exception raised when the disk isn't initialised.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class Disk:
    """Object for intefacing with a FAT32 disk.

    Attributes:
        reader (Callable[[int], bytes]): Function to read 512 bytes at a time from a given drive block.
        writer (Callable[[int, bytearray], Any]): Function to write 512 bytes at a time to a given drive block.
        partitions (List[Partition]): List of detected partitions on the disk.
        partition (Optional[Partition]): The currently selected partition.
        bios_parameter_block (Optional[BiosParameterBlock]): BIOS parameter block for the current partition.
        initialised (bool): Whether the disk has been initialised.
    """

    def __init__(
        self, reader: Callable[[int], bytes], writer: Callable[[int, bytearray], Any]
    ) -> None:
        """
        Initialize a Disk object for interfacing with a FAT32 disk.

        Parameters:
            reader (Callable[[int], bytes]): Function to read 512 bytes at a time from a given drive block.
            writer (Callable[[int, bytearray], Any]): Function to write 512 bytes at a time to a given drive block.
        Returns:
            None
        """
        self.reader = reader
        self.writer = writer
        self.partitions = []
        self.partition = None
        self.bios_parameter_block = None
        self.initialised = False

    # Private Instance Methods
    def _read_disk(self, offset: int) -> bytearray:
        """
        Read a block from disk at the given offset.

        Parameters:
            offset (int): The byte offset to read from.
        Returns:
            bytearray: The data read from disk.
        """
        block = offset // LOGICAL_BLOCK_SIZE
        return bytearray(self.reader(block))

    def _write_disk(self, offset: int, data: bytearray) -> Any:
        """
        Write a block to disk at the given offset.

        Parameters:
            offset (int): The byte offset to write to.
            data (bytearray): The data to write.
        Returns:
            Any: The result of the writer function.
        """
        block = offset // LOGICAL_BLOCK_SIZE
        return self.writer(block, data)

    def _get_partitions(self) -> List[Partition]:
        """
        Retrieve all partitions from the disk.

        Returns:
            List[Partition]: List of Partition objects found on disk.
        """
        data = self._read_disk(0)
        return Partition.parse_partitions(data)

    def _get_bios_parameter_block(self) -> BiosParameterBlock:
        """
        Read and return the BIOS parameter block for the current partition.

        Returns:
            BiosParameterBlock: The BIOS parameter block object.
        """
        partition_offset = self.partition.get_partition_offset()
        data = self._read_disk(partition_offset)
        return BiosParameterBlock(data)

    def _get_root_directory_entries(self) -> List[File]:
        """
        Get all root directory entries as File objects.

        Returns:
            List[File]: List of File objects in the root directory.
        """
        bytes_per_cluster = self.bios_parameter_block.get_bytes_per_cluster()

        data = bytearray()
        for chunk in self._read_file_in_chunks(
            # TODO: Figure out how to get the actual size of the directory "file"
            File(None, None, None, 2, bytes_per_cluster, None, None, None, None)
        ):
            data.extend(chunk)

        return File.parse_directory_entries(data)

    def _get_next_cluster(self, cluster: int) -> int:
        """
        Get the next cluster in the chain for a given cluster number.

        Parameters:
            cluster (int): The current cluster number.
        Returns:
            int: The next cluster number in the chain.
        """
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

    def _read_file_in_chunks(self, file: File) -> Generator[bytearray, None, None]:
        """
        Yield file data in chunks (by cluster) for the given File object.

        Parameters:
            file (File): The file to read.
        Yields:
            bytearray: Chunks of file data.
        """
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

    def _find_next_empty_fat_entry(
        self, cluster: int
    ) -> Tuple[bytearray, int, Optional[int]]:
        """
        Find the next empty FAT entry starting from the given cluster.

        Parameters:
            cluster (int): The cluster number to start searching from.
        Returns:
            Tuple[bytearray, int, Optional[int]]: FAT sector data, sector number, and index of empty entry.
        """
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

    def _write_fat_entry(
        self, data: bytearray, sector_num: int, idx: int, entry: bytes
    ) -> None:
        """
        Write a FAT entry to the FAT table for a given cluster.

        Parameters:
            data (bytearray): FAT sector data.
            sector_num (int): FAT sector number.
            idx (int): Index in the sector to write.
            entry (bytes): FAT entry value to write.
        Returns:
            None
        """
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

    def _get_fat_block_for_cluster(self, cluster: int) -> Tuple[bytearray, int, int]:
        """
        Get the FAT block, sector number, and index for a given cluster.

        Parameters:
            cluster (int): The cluster number to look up.
        Returns:
            Tuple[bytearray, int, int]: FAT sector data, sector number, and index for the cluster.
        """
        partition_offset = self.partition.get_partition_offset()
        fat_table_byte_offset = self.bios_parameter_block.get_fat_table_byte_offset()

        idx = (cluster * 4) % LOGICAL_BLOCK_SIZE
        sector_num = (cluster * 4) // LOGICAL_BLOCK_SIZE
        data = self._read_disk(
            partition_offset + fat_table_byte_offset + (sector_num * LOGICAL_BLOCK_SIZE)
        )

        return (data, sector_num, idx)

    def _allocate_next_free_cluster(self, last_cluster: int) -> None:
        """
        Allocate the next free cluster and update the FAT table.

        Parameters:
            last_cluster (int): The last cluster in the file's chain.
        Returns:
            None
        """
        data, sector_num, idx = self._find_next_empty_fat_entry(last_cluster)
        entry = (0x0FFFFFF8 & 0x0FFFFFFF).to_bytes(4, byteorder="little")
        self._write_fat_entry(data, sector_num, idx, entry)
        new_cluster = ((sector_num * LOGICAL_BLOCK_SIZE) + idx) // 4

        data, sector_num, idx = self._get_fat_block_for_cluster(last_cluster)

        entry = (new_cluster & 0x0FFFFFFF).to_bytes(4, byteorder="little")
        self._write_fat_entry(data, sector_num, idx, entry)

    def _get_files_last_cluster(self, file: File) -> int:
        """
        Get the last cluster in the chain for the given file.

        Parameters:
            file (File): The file object to check.
        Returns:
            int: The last cluster number in the file's chain.
        """
        cluster = file.start_cluster
        while cluster < 0x0FFFFFF8:
            next_cluster = self._get_next_cluster(cluster)
            if next_cluster < 0x0FFFFFF8:
                cluster = self._get_next_cluster(cluster)
            else:
                return cluster
        return cluster

    def _create_file_record(self, file: File) -> File:
        """
        Create a new file record (stub).

        Parameters:
            file (File): The file object to create.
        Returns:
            File: The created file object.
        """
        # TODO: Make this actually create the file record
        return file

    def _update_file_record(self, file: File, num_bytes_written: int) -> File:
        """
        Update the file record with the number of bytes written (stub).

        Parameters:
            file (File): The file object to update.
            num_bytes_written (int): Number of bytes written to the file.
        Returns:
            File: The updated file object.
        """
        # TODO: Make this actually update the file record
        file.size = file.size + num_bytes_written
        return file

    def _write_to_last_cluster(
        self, file: File, data: bytearray
    ) -> Tuple[bytearray, int]:
        """
        Write data to the last cluster of the file, allocating new clusters if needed.

        Parameters:
            file (File): The file object to write to.
            data (bytearray): The data to write.
        Returns:
            Tuple[bytearray, int]: Remaining data and number of bytes written.
        """
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

    def _append_to_file(self, file: File, data: bytearray) -> File:
        """
        Append data to the file, allocating clusters as needed.

        Parameters:
            file (File): The file object to append to.
            data (bytearray): The data to append.
        Returns:
            File: The updated file object.
        """
        while len(data) > 0:
            data, written = self._write_to_last_cluster(file, data)
            file.size = file.size + written
            if len(data) > 0:
                last_cluster = self._get_files_last_cluster(file)
                self._allocate_next_free_cluster(last_cluster)

        return file

    def init(self) -> None:
        """
        Initialise the disk by reading partitions and BIOS parameter block.

        Parameters:
            self (Disk): The disk object instance.
        Returns:
            None
        """
        self.partitions = self._get_partitions()
        self.partition = self._get_largest_non_empty_partition(self.partitions)
        self.bios_parameter_block = self._get_bios_parameter_block()
        self.initialised = True

    def list_root_files(self) -> List[File]:
        """
        List all files in the root directory.

        Parameters:
            self (Disk): The disk object instance.
        Returns:
            List[File]: List of File objects in the root directory.
        """
        if not self.initialised:
            raise DiskNotInitialised
        return self._get_root_directory_entries()

    def read_file_in_chunks(self, file: File) -> Generator[bytearray, None, None]:
        """
        Read a file in chunks (by cluster).

        Parameters:
            file (File): The file to read.
        Yields:
            bytearray: Chunks of file data.
        """
        if not self.initialised:
            raise DiskNotInitialised
        yield from self._read_file_in_chunks(file)

    def append_to_file(self, file: File, data: bytearray) -> File:
        """
        Append data to a file, allocating clusters as needed.

        Parameters:
            file (File): The file to append to.
            data (bytearray): The data to append.
        Returns:
            File: The updated file object.
        """
        if not self.initialised:
            raise DiskNotInitialised
        return self._append_to_file(file, data)

    @classmethod
    def _get_largest_non_empty_partition(cls, partitions: List[Partition]) -> Partition:
        """
        Return the largest non-empty partition from a list of partitions.

        Parameters:
            partitions (List[Partition]): List of Partition objects.
        Returns:
            Partition: The largest non-empty partition.
        """
        return sorted(partitions, key=lambda x: (x.num_sectors * -1, x.start_lba))[0]
