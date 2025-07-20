# fat32py

Minimal FAT32 Client written in Python

Still a work in progress, currently only reads from disk, can't yet write.

This is also a prototype for a version that will be writen in Rust which is being built to operate within some specific constraints imposed by running in an embedded systems environment.

**The following instructions assumes a Linux or Mac environment*

## Setup

Ensure you have python 3.13 installed and available. Create your `venv` as below:

```bash
$ python -m venv ./.venv
```

Activate it with:

```bash
$ source ./.venv/bin/activate
```

Ensure `uv` is installed by running:

```bash
$ pip install uv
```

From here run the following command to install the dependencies:

```bash
$ uv sync
```

## Running

To run the app simply run the below:

```bash
$ uv run main.py
```

Note: You will need to edit the name of the drive in the `main.py` to match a FAT32 disk on your own system, I recommend using an SDCard. If you plug one into you system, you can see the name for it by listing all the files in your `/dev` directory:

```bash
$ cd /dv/
$ ls
```
