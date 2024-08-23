class Kura:
    name: str

    def __init__(self, name: str = "kura") -> None:
        self.name = name

    def hello(self) -> str:
        return f"Hello from {self.name}!"


if __name__ == "__main__":
    kura = Kura()
    print(kura.hello())
