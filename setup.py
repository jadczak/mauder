from cx_Freeze import setup, Executable
from mauder import __version__
from sys import exit
import pathlib
import shutil


def main():
    name = "mauder"
    here = pathlib.Path(__file__).parent
    build_base_dir = here / r"build"
    build_dir = build_base_dir / r"mauder"
    dist_dir = here / r"dist"

    build_exe_options = {
        "build_exe": build_dir
    }

    if build_base_dir.exists():
        print("cleaning old build")
        shutil.rmtree(build_base_dir)
    build_dir.mkdir(parents=True)

    if dist_dir.exists():
        print("cleaning old dist")
        shutil.rmtree(dist_dir)
    dist_dir.mkdir(parents=True)

    print("running setup")
    setup(
        name=name,
        version=str(__version__),
        description="MAUDE database scraper",
        options={"build_exe": build_exe_options},
        executables=[Executable("mauder.py")]
    )

    data_dir = build_dir / "mdr-data-files"
    skeleton_dirs = [
        data_dir / "device",
        data_dir / "foitext",
        data_dir / "patientproblemdata",
        data_dir / "patientproblemcode",
    ]

    for skeleton_dir in skeleton_dirs:
        print(f"making skeleton directory {skeleton_dir}")
        skeleton_dir.mkdir(parents=True)

    print("zipping build for dist")
    shutil.make_archive(base_name=str(dist_dir / rf"{name}_{__version__}"), format="zip", root_dir=build_base_dir)

    exit(0)


if __name__ == "__main__":
    main()
