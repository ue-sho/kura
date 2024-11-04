import io
from pathlib import Path

from kura.file.block_id import BlockID
from kura.file.page import Page


class FileManager:
    db_dir: Path
    block_size: i64
    files: dict[str, io.BufferedRandom]

    def __init__(self, db_dir: str, block_size: i64):
        self.db_dir = Path(db_dir)
        self.block_size = block_size
        self.files: dict[str, io.BufferedRandom] = {}

        # Create db_dir if it does not exist
        self.db_dir.mkdir(parents=True, exist_ok=True)

        # Remove any leftover temporary tables
        for file in self.db_dir.iterdir():
            if file.is_file() and file.name.startswith("temp"):
                file.unlink()

    def block_size(self) -> i64:
        return self.block_size

    def read(self, blk: BlockID, p: Page) -> None:
        f = self.open_file(blk.filename)
        f.seek(blk.block_num * self.block_size)
        p.buffer[:] = f.read(len(p.buffer))

    def write(self, blk: BlockID, p: Page) -> None:
        f = self.open_file(blk.filename)
        f.seek(blk.block_num * self.block_size)
        f.write(p.buffer)

    def open_file(self, filename: str):
        if filename in self.files:
            return self.files[filename]

        file_path = self.db_dir / filename
        f = open(file_path, "r+b" if file_path.exists() else "w+b")
        self.files[filename] = f
        return f
