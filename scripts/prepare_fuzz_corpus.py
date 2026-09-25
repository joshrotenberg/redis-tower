#!/usr/bin/env python3
"""Materialize reviewed fuzz seed fixtures into a libFuzzer corpus."""

import argparse
from pathlib import Path

from fuzz_campaign import prepare_corpus


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("seed_dir", type=Path)
    parser.add_argument("corpus_dir", type=Path)
    args = parser.parse_args()
    prepare_corpus(args.seed_dir, args.corpus_dir)


if __name__ == "__main__":
    main()
