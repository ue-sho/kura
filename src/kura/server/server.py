from kura.file.file_manager import FileManager


class KuraDB:
    file_manager: FileManager

    def __init__(self, db_dir: str, block_size: i64):
        try:
            self.file_manager = FileManager(db_dir, block_size)
        except Exception as e:
            raise RuntimeError(f"file.NewManager error: {e}") from e
