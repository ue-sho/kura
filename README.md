# 蔵 (Kura)

蔵 is the next evolution of my previous RDBMS project, [お箱](https://github.com/ue-sho/ohako). While "お箱" meant a "box," "蔵" is more like a "warehouse". The goal of this project is to create a simple, yet functional, database management system from scratch in Python using [Codon](https://docs.exaloop.io/codon). The implementation is based on the SimpleDB of the book "[Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)". The original code of SimpleDB is written in Java and can be found [here](http://www.cs.bc.edu/~sciore/simpledb/).

## Setup

1. Install dependencies
    ```bash
    curl -sSf https://rye.astral.sh/get | bash
    /bin/bash -c "$(curl -fsSL https://exaloop.io/install.sh)"
    rye sync
    ```
1. Run the project
    ```bash
    make run

    # or
    make build
    ./kura
    ```
