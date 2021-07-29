import json
import os
import sys
from contextlib import closing, suppress
from functools import wraps
from pathlib import Path

from owl_dev import database as db

_author__ = "Eduardo Gonzalez Solares"
__email__ = "eglez@ast.cam.ac.uk"
__version__ = "0.1.0"


OWL_DONE = ".owl_completed"
SQLITEDB = "sqlite.db"


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Path):
            return obj.as_posix()
        return json.JSONEncoder.default(self, obj)


def pipeline(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        pdef = wrapper.config
        output_dir = kwds.get("output_dir")
        if output_dir is not None:
            with suppress(Exception):
                (output_dir / OWL_DONE).unlink()

            with suppress(Exception):
                (output_dir / SQLITEDB).unlink()

            db.init_database(f"sqlite:///{output_dir}/{SQLITEDB}")
            with open(f"{output_dir}/config.yaml", "w") as fh:
                fh.write(json.dumps(pdef))
            with open(f"{output_dir}/env.yaml", "w") as fh:
                fh.write(json.dumps(dict(os.environ)))

        else:
            db.init_database("sqlite:///:memory:")

        with closing(db.DBSession()) as session:
            info = db.Info(
                config=JSONEncoder().encode(pdef),
                env=JSONEncoder().encode(dict(os.environ)),
                python=sys.version,
            )
            session.add(info)
            session.commit()

        try:
            f(*args, **kwds)
        finally:
            if output_dir is not None:
                (output_dir / OWL_DONE).touch()

    return wrapper
