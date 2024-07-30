from sys import argv, exit
from time import time
import pathlib

# type aliases
MaudeData = dict[int, list[bytes]]
Header = list[bytes]
PatientCodes = dict[bytes, bytes]


def main(args: list):
    start = time()
    if not args:
        print_help()
        exit(0)
    if "-h" in args:
        print_help()
        print_long_help()
        exit(0)
    product_codes = {bytes(arg, encoding="utf-8") for arg in args}
    here = pathlib.Path(".")
    data_dir = here / "mdr-data-files"
    device_dir = data_dir / "device"
    foitext_dir = data_dir / "foitext"
    patient_codes_file = data_dir / "patientproblemdata/patientproblemcodes.csv"
    patient_problem_dir = data_dir / "patientproblemcode"
    maude_data, header = parse_device_files(device_dir, product_codes)
    maude_data, header = parse_foitext(foitext_dir, maude_data, header)
    length_check(maude_data, header)
    patient_codes = parse_patient_codes(patient_codes_file)
    maude_data, header = parse_patient_problems(patient_problem_dir, maude_data, header, patient_codes)
    length_check(maude_data, header)
    end = time()
    print(f"Elapsed time: {end - start}")
    # dump_key(maude_data, header)
    exit(0)


def length_check(maude_data: MaudeData, header: Header) -> None:
    """
    Sanity check to make sure that the data is well formed.  The header and
    the parsed MAUDE data were getting out of sync during development, so
    run this function any time you mutate the header or maude_data to see if
    things are still aligned.
    """
    for key in maude_data:
        try:
            assert len(header) == len(maude_data[key])
            break
        except AssertionError:
            print(f"Header length mismatch: {len(header)} != {len(maude_data[key])}")
            exit(0)


def dump_key(maude_data: MaudeData, header: Header, key: int = 0) -> None:
    """
    Helper function for printing out an MDR record.  If no MDR key is provided
    the first key from the maude_data is selected.
    """
    if not key:
        for key in maude_data:
            break
    for h, v in zip(header, maude_data[key]):
        print(f"{str(h):35}{str(v)}")


def fill_blank_data(new_data: MaudeData, size: int, keys_to_update: set) -> MaudeData:
    """
    The MAUDE database is a bit of a cluster at times.  Once we have the set of keys
    that we are focused on, any time we are going to parse a new dataset and add content
    we should be following up after the update to ensure that the column alignment
    doesn't get screwed if there is an errant index error while adding the new data,
    or if a report key is missing a particular bit of information.
    """
    for k in keys_to_update:
        new_data[k] = [b""] * size
    return new_data


def extend_data(maude_data: MaudeData, new_data: MaudeData) -> MaudeData:
    """
    This is more of the MAUDE files being wonky at times.  There are no guarentees
    that a key that is parsed from a given set of data is going to show up in the
    device files.  So as new data is parsed, we keep it separate and then combine
    with the original data after.
    """
    for key in maude_data.keys() & new_data.keys():
        maude_data[key].extend(new_data[key])
    return maude_data


def parse_device_files(path: pathlib.Path, product_codes: set[bytes]) -> tuple[MaudeData, Header]:
    """
    Searches through a folder and parses out data from device files for the product codes indicated.
    The MAUDE data can be screwy so we have to check for line length and deal with data showing
    up in the wrong locations.  The Device files seem to be the worst about malformed data.
    """
    # I thought about doing this dynamically, but screw it.
    # I'll fix it later if it becomes a problem.
    REPORT_KEY = 0
    PRODUCT_CODE = 25
    RN = -2
    change_file = None
    header = []
    line_len: int = -1
    maude_data: MaudeData = {}
    fast_codes: bool = False
    if len(product_codes) < 3:
        fast_codes = True
        product_codes = {b"|" + pc + b"|" for pc in product_codes}
    print("Searching for Device files")
    for file in path.iterdir():
        if "change" in file.name.lower():
            change_file = file
        elif not "DEVICE" in file.name.upper():
            print(f"Skipping non-device file {file.name}")
        else:
            if fast_codes:
                with open(file, "rb") as f:
                    print(f"reading file {file.name}")
                    first = f.readline()
                    if not header:
                        header = first[:RN].split(b"|")
                        line_len = len(header)
                    for line in f:
                        for product_code in product_codes:
                            if product_code in line:
                                split_line = line[:RN].split(b"|")
                                if len(split_line) != line_len:
                                    continue  # ditch malformed lines.
                                try:
                                    key = int(split_line[REPORT_KEY])
                                    maude_data[key] = split_line
                                except ValueError:
                                    # very seldom, the thing in the leftmost column isn't a number.
                                    # TODO: add some error logging here so we aren't failing siletly.
                                    pass
            else:
                with open(file, "rb") as f:
                    print(f"reading file {file.name}")
                    first = f.readline()
                    if not header:
                        header = first[:RN].split(b"|")
                        line_len = len(header)
                    for line in f:
                        split_line = line[:RN].split(b"|")
                        if len(split_line) != line_len:
                            continue
                        if split_line[PRODUCT_CODE] in product_codes:
                            try:
                                key = int(split_line[REPORT_KEY])
                                maude_data[key] = split_line
                            except ValueError:
                                # TODO: add errors
                                pass
    if change_file:
        with open(change_file, "rb") as f:
            print(f"reading file {change_file.name}")
            f.readline()
            for line in f:
                split_line = line[:RN].split(b"|")
                if len(split_line) != line_len:
                    continue  # ditch malformed lines.
                try:
                    key = int(split_line[REPORT_KEY])
                    if key in maude_data:
                        for x in range(1, line_len):
                            byte_string = b"\nChange:\n" + split_line[x]
                            maude_data[key][x - 1] += byte_string
                except ValueError:
                    print("error")
                    # TODO: add some error logging here so we aren't failing siletly?
                    pass

    return maude_data, header


def parse_foitext(path: pathlib.Path, maude_data: MaudeData, header: Header) -> tuple[MaudeData, Header]:
    """
    This parses out the foi text which includes all the narrative data (reporter and manufacturer lies)
    from the MAUDE records.  Missing records is fairly common here, so we have to populate blank data
    whenever there is a device record without corresponding foi data.
    """
    REPORT_KEY = 0
    RN = -2
    change_file = None
    header_add: Header = []
    new_data: MaudeData = {}
    line_len: int = -1
    print("Searching for foitext files")
    for file in path.iterdir():
        if "change" in file.name.lower():
            change_file = file
        elif not "foitext" in file.name:
            print(f"Skipping non-foitext file: {file.name}")
        else:
            with open(file, "rb") as f:
                print(f"reading file {file.name}")
                first = f.readline()
                if not header_add:
                    this_header = first[:RN].split(b"|")
                    line_len = len(this_header)
                    header_add = this_header[1:]
                for line in f:
                    split_line = line[:RN].split(b"|")
                    if len(split_line) != line_len:
                        continue
                    try:
                        key = int(split_line[REPORT_KEY])
                        if key in maude_data:
                            new_data[key] = split_line[1:]
                    except ValueError:
                        # TODO: Error logging.
                        pass

    # fill missing information
    keys_to_update = maude_data.keys() - new_data.keys()
    size = len(header_add)
    new_data = fill_blank_data(new_data, size, keys_to_update)

    if change_file:
        with open(change_file, "rb") as f:
            print(f"reading file {change_file.name}")
            first = f.readline()
            for line in f:
                split_line = line[:RN].split(b"|")
                if len(split_line) != line_len:
                    continue
                try:
                    key = int(split_line[REPORT_KEY])
                    if key in maude_data:
                        for x in range(1, line_len):
                            byte_string = b"\nChange:\n" + split_line[x]
                            new_data[key][x - 1] += byte_string
                except ValueError:
                    # TODO: Error logging.
                    pass
    maude_data = extend_data(maude_data, new_data)
    header.extend(header_add)
    return maude_data, header


def parse_patient_codes(patient_codes_file: pathlib.Path) -> PatientCodes:
    """
    Patient outcomes are encoded for reasons that are beyond me.  This creates
    the lookup for turning a patient code into the human readable translation.
    """
    RN = -2
    patient_codes = {}
    print(f"reading file {patient_codes_file.name}")
    with open(patient_codes_file, "rb") as f:
        for line in f:
            line = line[:RN]
            idx = line.find(b",")
            code = line[:idx]
            problem = line[idx + 1 :]  # skip the comma
            problem = problem.lstrip(b'"').rstrip(b'"')  # more MAUDE weirdness.
            patient_codes[code] = problem
    return patient_codes


def parse_patient_problems(
    path: pathlib.Path, maude_data: MaudeData, header: Header, patient_codes: PatientCodes
) -> tuple[MaudeData, Header]:
    """
    This parses the patient problems (outcomes) for the maude data.  Patient outcomes
    are all splatted into a single file instead of being broken up by year.
    The patientproblemcode.txt file is weird in a few ways.
    1)  the report keys are decimal instead of ints
    2)  report keys show up multiple times in the file because
        patients can have multiple problems associated with them
    3)  Any changes show up in this file instead of in a separate
        "change" file.
    """
    REPORT_KEY = 0
    PROBLEM_CODE = 2
    SPACE = 0
    DOT_ZERO = -2
    RN = -2
    new_data: MaudeData = {}
    header_add: Header = []
    line_len: int = -1
    print(f"Seaching for patient files")
    for file in path.iterdir():
        if not "patient" in file.name.lower():
            print(f"skipping non-patient file {file.name}")
        else:
            with open(file, "rb") as f:
                print(f"reading file {file.name}")
                first = f.readline()
                # there should only ever be one patientproblemcode.txt file
                # so this check is unnecessary, but the MAUDE database is funny sometimes
                # so guard against changes in the future.
                if not header_add:
                    this_header = first[:RN].split(b"|")
                    line_len = len(this_header)
                    header_add = this_header[1:]
                for line in f:
                    split_line = line[:RN].split(b"|")
                    if len(split_line) != line_len:
                        continue
                    try:
                        # slicing is faster than int(float(string))
                        key = int(split_line[REPORT_KEY][SPACE:DOT_ZERO])
                        if key in maude_data:
                            split_line[PROBLEM_CODE] = patient_codes[split_line[PROBLEM_CODE]]
                            if key in new_data:
                                for x in range(1, line_len):
                                    byte_string = b"\n" + split_line[x]
                                    new_data[key][x - 1] += byte_string
                            else:
                                new_data[key] = split_line[1:]
                    except IndexError:
                        # TODO: add some error logging or something.
                        pass
    # fill in the blanks
    keys_to_update = maude_data.keys() - new_data.keys()
    size = len(header_add)
    maude_data = fill_blank_data(maude_data, size, keys_to_update)
    header.extend(header_add)
    maude_data = extend_data(maude_data, new_data)
    return maude_data, header


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
