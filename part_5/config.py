from typing import Dict

import toml


def read_config() -> Dict:
    with open("config.toml") as f:
        return toml.load(f)


config = read_config()
