from kura.main import Kura


def test_kura_default_name():
    kura = Kura()
    assert kura.name == "kura"
    assert kura.hello() == "Hello from kura!"


def test_kura_custom_name():
    kura = Kura(name="custom")
    assert kura.name == "custom"
    assert kura.hello() == "Hello from custom!"
