from sys import argv, exit
from time import time, strftime
import argparse
import multiprocessing
import pathlib
import psutil
import textwrap

# type aliases
MaudeData = dict[int, list[bytes]]
Header = list[bytes]
PatientCodes = dict[bytes, bytes]


def main(args: list):
    arguments = parse_args(args)
    if arguments.more:
        print_long_help()
        parse_args(["-h"])
        exit(0)
    if not any(vars(arguments).values()) or arguments.procs < 1:
        parse_args(["-h"])

    start = 0
    end = 0
    write_end = 0
    here = pathlib.Path(".")
    data_dir = here / "mdr-data-files"
    device_dir = data_dir / "device"
    foitext_dir = data_dir / "foitext"
    patient_codes_file = data_dir / "patientproblemdata/patientproblemcodes.csv"
    patient_problem_dir = data_dir / "patientproblemcode"

    if arguments.codes:
        if arguments.test:
            start = time()
        product_codes = {bytes(arg, encoding="utf-8") for arg in arguments.codes}

        n_chunks = arguments.procs
        maude_data, header = parse_device_files(device_dir, product_codes, n_chunks)
        maude_data, header = parse_foitext(foitext_dir, maude_data, header, n_chunks)
        patient_codes = parse_patient_codes(patient_codes_file)
        maude_data, header = parse_patient_problems(patient_problem_dir, maude_data, header, patient_codes, n_chunks)
        if arguments.test:
            end = time()
        codes = "-".join([c for c in arguments.codes])
        file = pathlib.Path(f"{strftime("%Y%m%d%H%M%S")}-{codes}.txt")
        maude_data, header = convert_bytes_to_strings(maude_data, header)
        write_maude_data(file, maude_data, header)
        if arguments.test:
            write_end = time()
    else:
        print("No product codes provided.")

    if arguments.test:
        total_size, read_time = test_speed([device_dir, foitext_dir, patient_problem_dir, patient_codes_file])
        read_throughput = total_size / read_time / 2**30
        read_efficiency = read_throughput / read_throughput
        parsing_time = end - start
        parsing_throughput = total_size / parsing_time / 2**30
        parsing_efficiency = parsing_throughput / read_throughput
        writing_time = write_end - end
        total_time = write_end - start
        print()
        print(f"{'MODE':20}{'TIME (s)':20}{'THROUGHPUT GB/s':20}{'EFFICIENCY':20}")
        print(f"{'Raw Reading':20}{read_time:<20.3f}{read_throughput:<20.3f}{read_efficiency:<20.2%}")
        if parsing_time:
            print(f"{'File Parsing':20}{parsing_time:<20.3f}{parsing_throughput:<20.3f}{parsing_efficiency:<20.2%}")
            print(f"{'Multiprocessing pool size':40}{arguments.procs}")
            print(f"{'Time to write excel file':40}{writing_time:.3f}")
            print(f"{'Total processing time':40}{total_time:.3f}")
        else:
            print(f"{'N/A':20}{0:20.3f}{0:20.3f}{0:20.2%}")
        print(f"{'Total size of processed files':40}{total_size / 2**30:.3f} GB")

    exit(0)


def convert_bytes_to_strings(maude_data: MaudeData, header: Header) -> tuple[MaudeData, Header]:
    """
    excel writers don't allow passing 'errors' values for managing non utf-8 characters.
    """
    print("converting bytes to string")
    for key in maude_data:
        byte_data = maude_data[key]
        str_data = [b.decode("utf-8", "ignore") for b in byte_data]  # type: ignore
        maude_data[key] = str_data  # type: ignore

    header = [b.decode("utf-8", "ignore") for b in header]  # type: ignore
    return maude_data, header


def write_maude_data(file: pathlib.Path, maude_data: MaudeData, header: Header) -> None:
    """
    dump maude data to file
    """
    print("writing output to disk")
    total_lines = len(maude_data)
    with open(file, "w", encoding="utf-8") as f:
        f.write("\t".join(header))  # type: ignore
        f.write("\n")
        for i, key in enumerate(sorted(maude_data), start=1):
            f.write("\t".join(maude_data[key]))  # type: ignore
            f.write("\n")
            if i % 10_000 == 0:
                print(f"writing line {i} of {total_lines}")


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
        maude_data[key].extend(new_data[key][1:])
    return maude_data


def chunk_file(file: pathlib.Path, n_chunks: int) -> list[tuple[int, int]]:
    """
    Splits up a file based on the number of chunks requested (ditching the header)
    The number of chunks will be the size of the multiprocessing pool.
    """
    file_size = file.stat().st_size
    chunk_size = file_size // n_chunks
    end_boundaries = [i * chunk_size for i in range(1, n_chunks)]
    with open(file, "rb") as f:
        header = f.readline()
        header_len = len(header)
        for i, end_boundary in enumerate(end_boundaries):
            f.seek(end_boundary)
            char = f.read(1)
            while char != b"\n":
                end_boundary += 1
                char = f.read(1)
            end_boundaries[i] = end_boundary
    start_boundaries = [header_len] + [eb + 1 for eb in end_boundaries]
    end_boundaries.append(file_size)
    file_locations = list(zip(start_boundaries, end_boundaries))
    return file_locations


def get_header(file: pathlib.Path) -> Header:
    RN = -2
    with open(file, "rb") as f:
        header = f.readline()[:RN].split(b"|")
    return header


def parse_device_files(path: pathlib.Path, product_codes: set[bytes], n_chunks: int) -> tuple[MaudeData, Header]:
    """
    Searches through a folder and parses out data from device files for the product codes indicated.
    The MAUDE data can be screwy so we have to check for line length and deal with data showing
    up in the wrong locations.  The Device files seem to be the worst about malformed data.
    """
    # I thought about doing this dynamically, but screw it.
    # I'll fix it later if it becomes a problem.
    change_file = None
    header: Header = []
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
        elif "DEVICE" not in file.name.upper():
            print(f"Skipping non-device file {file.name}")
        else:
            print(f"reading device file: {file.name}")
            if not header:
                header = get_header(file)
                line_len = len(header)
            locations = chunk_file(file, n_chunks)
            tasks = []
            for start, end in locations:
                tasks.append([file, start, end, product_codes, fast_codes, line_len])
            with multiprocessing.Pool(n_chunks) as pool:
                chunk_results = pool.starmap(parse_device_chunk, tasks)
            for chunk_result in chunk_results:
                maude_data.update(chunk_result)

    if change_file:
        print(f"reading device file: {change_file.name}")
        locations = chunk_file(change_file, n_chunks)
        tasks = []
        maude_keys = set(maude_data.keys())
        for start, end in locations:
            tasks.append([change_file, start, end, product_codes, line_len])
        with multiprocessing.Pool(n_chunks) as pool:
            chunk_results = pool.starmap(parse_general_chunk, tasks)
        for chunk_result in chunk_results:
            for key in chunk_result.keys() & maude_keys:
                for i in range(1, line_len):
                    byte_string = b"  Change: " + chunk_result[key][i]
                    maude_data[key][i] += byte_string

    return maude_data, header


def parse_device_chunk(
    file: pathlib.Path, start: int, end: int, product_codes: set[bytes], fast_codes: bool, line_len: int
) -> MaudeData:
    """
    Helper for parsing the device data across multiple processes.
    """
    if fast_codes:
        maude_data = parse_device_chunk_fast_codes(file, start, end, product_codes, line_len)
    else:
        maude_data = parse_device_chunk_reg_codes(file, start, end, product_codes, line_len)
    return maude_data


def parse_device_chunk_fast_codes(
    file: pathlib.Path, start: int, end: int, product_codes: set[bytes], line_len: int
) -> MaudeData:
    """
    Fast parsing of device data looking for product codes in the line.
    Looks for device data between the specified start and end bytes in the file.
    """
    RN = -2
    REPORT_KEY = 0
    maude_data: MaudeData = {}
    pos: int = start
    with open(file, "rb") as f:
        f.seek(start)
        while pos < end:
            line = f.readline()
            pos += len(line)
            for product_code in product_codes:  # TODO: should I special case for len == 1?
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

    return maude_data


def parse_device_chunk_reg_codes(
    file: pathlib.Path, start: int, end: int, product_codes: set[bytes], line_len: int
) -> MaudeData:
    """
    Normal parsing of device data looking for line's product code in the set of product codes.
    Looks for device data between the specified start and end bytes in the file.
    """
    RN = -2
    REPORT_KEY = 0
    PRODUCT_CODE = 25
    maude_data: MaudeData = {}
    pos: int = start
    with open(file, "rb") as f:
        f.seek(start)
        while pos < end:
            line = f.readline()
            pos += len(line)
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

    return maude_data


def parse_general_chunk(file: pathlib.Path, start: int, end: int, keys: set[int], line_len: int) -> MaudeData:
    """
    File parsing based on the specifed start and end bytes in the file.
    """
    RN = -2
    REPORT_KEY = 0
    maude_data: MaudeData = {}
    pos: int = start
    with open(file, "rb") as f:
        f.seek(start)
        while pos < end:
            line = f.readline()
            pos += len(line)
            split_line = line[:RN].split(b"|")
            if len(split_line) != line_len:
                continue  # ditch malformed lines.
            try:
                key = int(split_line[REPORT_KEY])
                if key in keys:
                    maude_data[key] = split_line
            except ValueError:
                # TODO: add some error logging here so we aren't failing siletly?
                pass
    return maude_data


def parse_foitext(path: pathlib.Path, maude_data: MaudeData, header: Header, n_chunks: int) -> tuple[MaudeData, Header]:
    """
    This parses out the foi text which includes all the narrative data (reporter and manufacturer lies)
    from the MAUDE records.  Missing records is fairly common here, so we have to populate blank data
    whenever there is a device record without corresponding foi data.
    """
    change_file = None
    header_add: Header = []
    new_data: MaudeData = {}
    line_len: int = -1
    maude_keys: set[int] = set(maude_data.keys())  # can't pickle dict_keys object for starmap.
    print("Searching for foi text files")
    for file in path.iterdir():
        if "change" in file.name.lower():
            change_file = file
        elif "foitext" not in file.name:
            print(f"Skipping non-foitext file: {file.name}")
        else:
            print(f"reading foi text file: {file.name}")
            if not header_add:
                this_header = get_header(file)
                line_len = len(this_header)
                header_add = this_header[1:]
            locations = chunk_file(file, n_chunks)
            tasks = []
            for start, end in locations:
                tasks.append([file, start, end, maude_keys, line_len])
            with multiprocessing.Pool(n_chunks) as pool:
                chunk_results = pool.starmap(parse_general_chunk, tasks)
            for chunk_result in chunk_results:
                new_data.update(chunk_result)

    # fill missing information
    keys_to_update = maude_keys - new_data.keys()
    size = len(header_add)
    new_data = fill_blank_data(new_data, size, keys_to_update)

    if change_file:
        print(f"reading foi text file: {change_file.name}")
        locations = chunk_file(change_file, n_chunks)
        tasks = []
        for start, end in locations:
            tasks.append([change_file, start, end, maude_keys, line_len])
        with multiprocessing.Pool(n_chunks) as pool:
            chunk_results = pool.starmap(parse_general_chunk, tasks)
        for chunk_result in chunk_results:
            for key in chunk_result.keys() & maude_keys:
                for i in range(1, line_len):
                    byte_string = b"  Change: " + chunk_result[key][i]
                    new_data[key][i] += byte_string

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
    print(f"reading patient code file: {patient_codes_file.name}")
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
    path: pathlib.Path, maude_data: MaudeData, header: Header, patient_codes: PatientCodes, n_chunks: int
) -> tuple[MaudeData, Header]:
    """
    This parses the patient problems (outcomes) for the maude data.  Patient outcomes
    are all splatted into a single file instead of being broken up by year.
    """
    new_data: MaudeData = {}
    header_add: Header = []
    line_len: int = -1
    maude_keys: set[int] = set(maude_data.keys())  # can't pickle dict_keys for starmap.
    print("Seaching for patient files")
    for file in path.iterdir():
        if "patient" not in file.name.lower():
            print(f"skipping non-patient file {file.name}")
        else:
            print(f"reading patient problem file: {file.name}")
            if not header_add:
                this_header = get_header(file)
                line_len = len(this_header)
                header_add = this_header[1:]
            locations = chunk_file(file, n_chunks)
            tasks = []
            for start, end in locations:
                tasks.append([file, start, end, maude_keys, line_len, patient_codes])
            with multiprocessing.Pool(n_chunks) as pool:
                chunk_results = pool.starmap(parse_patient_chunk, tasks)
            for chunk_result in chunk_results:
                # theoretically this song and dance is not needed since there should
                # only ever be one file here, but you never known how the maude
                # database will change, and I've already been bitten by crap like
                # this.  So we parse like there will someday be multiple files.
                new_data.update(chunk_result)
    # fill in the blanks
    keys_to_update = maude_keys - new_data.keys()
    size = len(header_add)
    new_data = fill_blank_data(new_data, size, keys_to_update)
    header.extend(header_add)
    maude_data = extend_data(maude_data, new_data)
    return maude_data, header


def parse_patient_chunk(
    file: pathlib.Path, start: int, end: int, keys: set[int], line_len: int, patient_codes: PatientCodes
) -> MaudeData:
    """
    The patientproblemcode.txt file is weird in a few ways.
    1)  the report keys are decimal instead of ints
    2)  report keys show up multiple times in the file because
        patients can have multiple problems associated with them
    3)  Any changes show up in this file instead of in a separate
        "change" file.
    """

    RN = -2
    DOT_ZERO = -2
    REPORT_KEY = 0
    SPACE = 0
    PROBLEM_CODE = 2
    new_data: MaudeData = {}
    pos: int = start
    with open(file, "rb") as f:
        f.seek(start)
        while pos < end:
            line = f.readline()
            pos += len(line)
            split_line = line[:RN].split(b"|")
            if len(split_line) != line_len:
                continue
            try:
                # slicing is faster than int(float(string))
                key = int(split_line[REPORT_KEY][SPACE:DOT_ZERO])
                if key in keys:
                    split_line[PROBLEM_CODE] = patient_codes[split_line[PROBLEM_CODE]]
                    if key in new_data:
                        for x in range(1, line_len):
                            byte_string = b"  " + split_line[x]
                            new_data[key][x] += byte_string
                    else:
                        new_data[key] = split_line

            except IndexError:
                # TODO: add some error logging.
                pass
    return new_data


def test_speed(paths: list[pathlib.Path]) -> tuple[int, float]:
    """
    Figure out how fast raw reads are of all the files to get an idea
    of the upper limit of performance on the target machine.
    """
    file_size = 0
    files = []
    for path in paths:
        if path.is_file():
            print(f"TEST: Adding\t{path.name}")
            file_size += path.stat().st_size
            files.append(open(path, "rb"))
        else:
            for file in path.iterdir():
                if ".gitkeep" not in file.name:
                    print(f"TEST: Adding\t{file.name}")
                    file_size += file.stat().st_size
                    files.append(open(file, "rb"))
    start = time()
    for file in files:
        file.read()
    end = time()
    for file in files:
        file.close()
    elapsed = end - start
    return file_size, elapsed


def parse_args(args: list[str]) -> argparse.Namespace:
    description = textwrap.dedent(
        """\
    Example:
        python mauder.py -c OYC LGZ QFG
        This will search through the available database files for all complaints
        containing any of the product codes: OYC, LGZ or QFG
    """
    )

    parser = argparse.ArgumentParser(
        prog="mauder.py", formatter_class=argparse.RawDescriptionHelpFormatter, description=description
    )
    parser.add_argument("-c", "--codes", nargs="+", default=[], type=str, dest="codes")
    parser.add_argument(
        "-m", "--more", help="Prints the extended help", default=False, action="store_true", dest="more"
    )
    parser.add_argument(
        "-t", "--test", help="Tests speed against raw read", default=False, action="store_true", dest="test"
    )
    parser.add_argument("-p", "--processes", default=psutil.cpu_count(logical=False), type=int, dest="procs")
    return parser.parse_args(args)


def print_long_help():
    long_help = textwrap.dedent(
        """\
    This utility searches the mrd-data-files directory for all product codes
    provided, aggregating useful information and exporting it as an excel
    document and also as a python pickle file containing a pandas dataframe
    of the aggregated information

    Maude data can be downloaded from the FDA's website in at the following location:

    https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files

    for the utility to work files need to be placed in the skeleton director as follows:
    .
    └── mdr-data-files/
        ├── device
        |   ├── DEVICE.txt
        |   ├── DEVICE2023.txt
        |   ├── DEVICE2022.txt
        |   ├── ...
        |   └── DEVICEChange.txt
        ├── foitext
        |   ├── foitext.txt
        |   ├── foitext2023.txt
        |   ├── ...
        |   └── foitextChange.txt
        ├── patientproblemcode
        |   └── patientproblemcode.txt
        └── patientproblemdata
            └── patientproblemcodes.csv

    NOTE: the 'patientproblemdata.zip' archive contains the file named 'patientproblemcodes.csv'.

    This utility will scan all available files.  Only include data as far back as you need or
    it may take a long time to run."""
    )
    print(long_help)


if __name__ == "__main__":
    main(argv[1:])
