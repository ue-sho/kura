from pathlib import Path

from kura.file.block_id import BlockID
from kura.file.page import Page
from kura.server.server import KuraDB


def test_file():
    temp_dir = Path("temp").joinpath("testfile")
    db = KuraDB(str(temp_dir), 400)

    file_manager = db.file_manager

    p1 = Page(file_manager.block_size)
    pos1 = 88
    str_val = "abcdefghijklm"
    p1.set_string(pos1, str_val)

    size = Page.max_length(len(str_val))
    pos2 = pos1 + size
    int_val = 345
    p1.set_int(pos2, int_val)

    blk = BlockID("testfile", 2)
    file_manager.write(blk, p1)

    p2 = Page(file_manager.block_size)
    file_manager.read(blk, p2)

    assert p2.get_int(pos2) == int_val
    assert p2.get_string(pos1) == str_val
    assert p2.get_string(pos1) == str_val
