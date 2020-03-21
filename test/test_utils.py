import os
import pathlib
from steer import utils


def test_get_dir(tmp_path):
    # note: tmp_path uses pathlib, prefer it to tmpdir
    root, project, kind, source = "data", "pertinent", "external", "peerless"
    p = tmp_path.joinpath(root, project, kind, source)
    p.mkdir(parents=True, exist_ok=True)

    assert p.is_dir()
    actual = utils.get_dir(tmp_path.joinpath(root), project, kind, source)
    assert actual.is_dir()
