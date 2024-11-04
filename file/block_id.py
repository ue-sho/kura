class BlockID:
    filename: str
    block_num: i64

    def __init__(self, filename: str, block_num: i64):
        self.filename = filename
        self.block_num = block_num

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BlockID):
            return NotImplemented
        return self.filename == other.filename and self.block_num == other.block_num

    def __str__(self) -> str:
        return f"{self.filename}.{self.block_num}"

    def __hash__(self) -> int:
        return hash(str(self))
