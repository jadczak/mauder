from sys import argv, exit
from typing import Generator
import mmap
import pathlib

# type aliases
MaudeData = dict[str, list[str]]


def main(args: list):
    if not args:
        print_help()
        exit(0)
    if "-h" in args:
        print_help()
        print_long_help()
        exit(0)
    product_codes = set(args)
    here = pathlib.Path(".")
    data_dir = here / "mdr-data-files"
    device_dir = data_dir / "device"
    foitext_dir = data_dir / "foitext"
    patient_codes_file = data_dir / "patientproblemdata/patientproblemcodes.csv"
    maude_data, header = parse_device_files(device_dir, product_codes)
    maude_data, header = parse_foitext(foitext_dir, maude_data, header)
    patient_codes = parse_patient_codes(patient_codes_file)
    length_check(maude_data, header)

    exit(0)


def length_check(maude_data: MaudeData, header: list[str]) -> None:
    for key in maude_data:
        try:
            assert len(header) == len(maude_data[key])
            break
        except AssertionError:
            print(f"Header length mismatch: {len(header)} != {len(maude_data[key])}")
            exit(0)


def fill_blank_data(new_data: MaudeData, size: int, keys_to_update: set) -> MaudeData:
    """
    The MAUDE database is a bit of a cluster at times.  Once we have the set of keys
    that we are focused on, any time we are going to parse a new dataset and add content
    we should be following up after the update to ensure that the column alignment
    doesn't get screwed if there is an errant index error while adding the new data,
    or if a report key is missing a particular bit of information.
    """
    for k in keys_to_update:
        new_data[k].extend([""] * size)
    return new_data


def extend_data(maude_data: MaudeData, new_data: MaudeData) -> MaudeData:
    for key in maude_data:
        maude_data[key].extend(new_data[key])
    return maude_data


def parse_device_files(path: pathlib.Path, product_codes: set[str]) -> tuple[MaudeData, list[str]]:
    # I thought about doing this dynamically, but screw it.
    # I'll fix it later if it becomes a problem.
    REPORT_KEY = 0
    PRODUCT_CODE = 25
    change_file = None
    header = []
    maude_data: MaudeData = {}
    print("Searching for Device files")
    for file in path.iterdir():
        if "Change" in file.name:
            change_file = file
        elif not "DEVICE" in file.name.upper():
            print(f"Skipping non-device file {file.name}")
        else:
            with open(file, "r") as f:
                print(f"reading file {file.name}")
                i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                first = i.readline()
                if not header:
                    header = first.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                for line in iter(i.readline, b""):
                    split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                    try:
                        if split_line[PRODUCT_CODE] in product_codes:
                            maude_data[split_line[REPORT_KEY]] = split_line
                    except IndexError:
                        # occasionally we get lines that aren't long enough.
                        # TODO: add some error logging here so we aren't failing silently?
                        pass
        if change_file:
            with open(change_file, "r") as f:
                print(f"reading file {change_file.name}")
                i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                i.readline()
                for line in iter(i.readline, b""):
                    split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                    try:
                        key = split_line[REPORT_KEY]
                        if key in maude_data:
                            for x in range(1, len(split_line)):
                                maude_data[key][x] += f"\nChange:\n{split_line[x]}"
                    except IndexError:
                        # TODO: add some error logging here so we aren't failing siletly?
                        pass

    return maude_data, header


def parse_foitext(path: pathlib.Path, maude_data: MaudeData, header: list[str]) -> tuple[MaudeData, list[str]]:
    REPORT_KEY = 0
    change_file = None
    header_add: list[str] = []
    new_data: MaudeData = {}
    print("Searching for foitext files")
    for file in path.iterdir():
        if "Change" in file.name.upper():
            change_file = file
        elif not "foitext" in file.name:
            print(f"Skipping non-foitext file: {file.name}")
        else:
            with open(file, "r") as f:
                print(f"reading file {file.name}")
                i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                first = i.readline()
                if not header_add:
                    header_add = first.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")[1:]
                for line in iter(i.readline, b""):
                    try:
                        split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                        key = split_line[REPORT_KEY]
                        if key in maude_data:
                            new_data[key] = split_line[1:]

                    except IndexError:
                        # TODO: add some error logging here so we aren't failing silently.
                        pass
    # fill missing information
    keys_to_update = maude_data.keys() - new_data.keys()
    size = len(header_add) - 1
    new_data = fill_blank_data(new_data, size, keys_to_update)

    if change_file:
        with open(change_file, "r") as f:
            print(f"reading file {change_file.name}")
            i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            first = i.readline()
            for line in iter(i.readline, b""):
                try:
                    split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                    key = split_line[REPORT_KEY]
                    if key in maude_data:
                        for x in range(1, len(split_line)):
                            new_data[key][x] += f"\nChange:\n{split_line[x]}"
                except IndexError:
                    # TODO: add some error logging here so we aren't failing silently.
                    pass
    maude_data = extend_data(maude_data, new_data)
    header.extend(header_add)
    return maude_data, header


def parse_patient_codes(patient_codes_file: pathlib.Path) -> dict[str, str]:
    patient_codes = {}
    print(f"reading file {patient_codes_file.name}")
    with open(patient_codes_file, "r") as f:
        i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for line in iter(i.readline, b""):
            line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n")
            idx = line.find(",")
            code = line[:idx]
            problem = line[idx + 1 :]  # skip the comma
            problem = problem.lstrip('"').rstrip('"')  # more MAUDE weirdness.
            patient_codes[code] = problem
    return patient_codes


def parse_patient_problems(
    patient_problems_file: pathlib.Path, maude_data: MaudeData, header: list[str], patient_codes: dict[str, str]
) -> tuple[MaudeData, list[str]]:
    thing: MaudeData = {}
    other: list[str] = []
    return (thing, other)


def print_help():
    print("Usage: python mauder.py [DEVICE CODES]")
    print()
    print("\t-h")
    print("\t\tPrints the extended help.")
    print()
    print("Example:")
    print("\tpython mauder.py OYC LGZ QFG")
    print("\tThis will search through the avilable database files for all complaints")
    print("\tcontaining any of the product codes: OYC LGZ or QFG")


def print_long_help():
    print()
    print("This utility searches the mrd-data-files directory for all product codes")
    print("provided, aggregating useful information and exporting it as an excel")
    print("document and also as a python pickle file containing a pandas dataframe")
    print("of the aggregated information")
    print()
    print("Maude data can be downloaded from the FDA's website in at the following location:")
    print(
        "https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files"
    )
    print("for the utility to work files need to be placed in the skeleton director as follows:")
    print()
    print("\t.")
    print("\t└── mdr-data-files/")
    print("\t    ├── device")
    print("\t    |   ├── DEVICE.txt")
    print("\t    |   ├── DEVICE2023.txt")
    print("\t    |   ├── DEVICE2022.txt")
    print("\t    |   ├── ...")
    print("\t    |   └── DEVICEChange.txt")
    print("\t    ├── deviceproblemcodes")
    print("\t    |   └── deviceproblemcodes.csv")
    print("\t    ├── foitext")
    print("\t    |   ├── foitext.txt")
    print("\t    |   ├── foitext2023.txt")
    print("\t    |   ├── ...")
    print("\t    |   └── foitextChange.txt")
    print("\t    ├── mdrfoi")
    print("\t    |   └── [APJ] Maybe not needed?")
    print("\t    ├── patient")
    print("\t    |   ├── patient.txt")
    print("\t    |   ├── ...")
    print("\t    |   └── patientchange.txt")
    print("\t    ├── patientproblemcode")
    print("\t    |   └── patientproblemcode.txt")
    print("\t    └── patientproblemdata")
    print("\t        └── patientproblemcodes.csv")
    print()
    print("NOTE: the 'patientproblemdata.zip' archive contains the file named 'patientproblemcodes.csv'.")
    print()
    print("This utility will scan all available files.  Only include data as far back as you need or")
    print("it may take a long time to run.")


if __name__ == "__main__":
    main(argv[1:])
