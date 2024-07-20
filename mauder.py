from sys import argv, exit
from typing import Generator
import mmap
import pathlib


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
    maude_data, header = parse_device_files(device_dir, product_codes)
    length_check(maude_data, header)
    maude_data, header = parse_foitext(foitext_dir, maude_data, header)
    length_check(maude_data, header)

    exit(0)


def length_check(maude_data: dict[str, list[str]], header: list[str]) -> None:
    for key in maude_data:
        try:
            assert len(header) == len(maude_data[key])
            break
        except AssertionError:
            print(f"Header length mismatch: {len(header)} != {len(maude_data[key])}")
            exit(0)


def indexer() -> Generator[int, None, None]:
    x = 0
    while True:
        yield x
        x += 1


def pre_fill(maude_data: dict[str, list[str]], size: int) -> dict[str, list[str]]:
    """
    The MAUDE database is a bit of a cluster at times.  Once we have the set of keys
    that we are focused on, any time we are going to parse a new dataset and add content
    we should be extending the list pre-emptively to ensure that the column alignment
    doesn't get screwed if there is an errant index error while adding the new data.
    """
    for k in maude_data:
        maude_data[k].extend([""] * size)
    return maude_data


def parse_device_files(path: pathlib.Path, product_codes: set[str]) -> tuple[dict[str, list[str]], list[str]]:
    idx = indexer()
    MDR_REPORT_KEY = next(idx)
    DEVICE_EVENT_KEY = next(idx)
    IMPLANT_FLAG = next(idx)
    DATE_REMOVED_FLAG = next(idx)
    DEVICE_SEQUENCE_NO = next(idx)
    DATE_RECEIVED = next(idx)
    BRAND_NAME = next(idx)
    GENERIC_NAME = next(idx)
    MANUFACTURER_D_NAME = next(idx)
    MANUFACTURER_D_ADDRESS_1 = next(idx)
    MANUFACTURER_D_ADDRESS_2 = next(idx)
    MANUFACTURER_D_CITY = next(idx)
    MANUFACTURER_D_STATE_CODE = next(idx)
    MANUFACTURER_D_ZIP_CODE = next(idx)
    MANUFACTURER_D_ZIP_CODE_EXT = next(idx)
    MANUFACTURER_D_COUNTRY_CODE = next(idx)
    MANUFACTURER_D_POSTAL_CODE = next(idx)
    DEVICE_OPERATOR = next(idx)
    EXPIRATION_DATE_OF_DEVICE = next(idx)
    MODEL_NUMBER = next(idx)
    CATALOG_NUMBER = next(idx)
    LOT_NUMBER = next(idx)
    OTHER_ID_NUMBER = next(idx)
    DEVICE_AVAILABILITY = next(idx)
    DATE_RETURNED_TO_MANUFACTURER = next(idx)
    DEVICE_REPORT_PRODUCT_CODE = next(idx)
    DEVICE_AGE_TEXT = next(idx)
    DEVICE_EVALUATED_BY_MANUFACTUR = next(idx)
    COMBINATION_PRODUCT_FLAG = next(idx)
    UDI_DI = next(idx)
    UDI_PUBLIC = next(idx)

    change_file = None
    header = []
    maude_data: dict[str, list[str]] = {}
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
                        if split_line[DEVICE_REPORT_PRODUCT_CODE] in product_codes:
                            maude_data[split_line[MDR_REPORT_KEY]] = [
                                split_line[MDR_REPORT_KEY],
                                split_line[DEVICE_EVENT_KEY],
                                split_line[IMPLANT_FLAG],
                                split_line[DATE_REMOVED_FLAG],
                                split_line[DEVICE_SEQUENCE_NO],
                                split_line[DATE_RECEIVED],
                                split_line[BRAND_NAME],
                                split_line[GENERIC_NAME],
                                split_line[MANUFACTURER_D_NAME],
                                split_line[MANUFACTURER_D_ADDRESS_1],
                                split_line[MANUFACTURER_D_ADDRESS_2],
                                split_line[MANUFACTURER_D_CITY],
                                split_line[MANUFACTURER_D_STATE_CODE],
                                split_line[MANUFACTURER_D_ZIP_CODE],
                                split_line[MANUFACTURER_D_ZIP_CODE_EXT],
                                split_line[MANUFACTURER_D_COUNTRY_CODE],
                                split_line[MANUFACTURER_D_POSTAL_CODE],
                                split_line[DEVICE_OPERATOR],
                                split_line[EXPIRATION_DATE_OF_DEVICE],
                                split_line[MODEL_NUMBER],
                                split_line[CATALOG_NUMBER],
                                split_line[LOT_NUMBER],
                                split_line[OTHER_ID_NUMBER],
                                split_line[DEVICE_AVAILABILITY],
                                split_line[DATE_RETURNED_TO_MANUFACTURER],
                                split_line[DEVICE_REPORT_PRODUCT_CODE],
                                split_line[DEVICE_AGE_TEXT],
                                split_line[DEVICE_EVALUATED_BY_MANUFACTUR],
                                split_line[COMBINATION_PRODUCT_FLAG],
                                split_line[UDI_DI],
                                split_line[UDI_PUBLIC],
                            ]
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
                        key = split_line[MDR_REPORT_KEY]
                        if key in maude_data:
                            maude_data[key][DEVICE_EVENT_KEY] += f"\nChange:\n{split_line[DEVICE_EVENT_KEY]}"
                            maude_data[key][IMPLANT_FLAG] += f"\nChange:\n{split_line[IMPLANT_FLAG]}"
                            maude_data[key][DATE_REMOVED_FLAG] += f"\nChange:\n{split_line[DATE_REMOVED_FLAG]}"
                            maude_data[key][DEVICE_SEQUENCE_NO] += f"\nChange:\n{split_line[DEVICE_SEQUENCE_NO]}"
                            maude_data[key][DATE_RECEIVED] += f"\nChange:\n{split_line[DATE_RECEIVED]}"
                            maude_data[key][BRAND_NAME] += f"\nChange:\n{split_line[BRAND_NAME]}"
                            maude_data[key][GENERIC_NAME] += f"\nChange:\n{split_line[GENERIC_NAME]}"
                            maude_data[key][MANUFACTURER_D_NAME] += f"\nChange:\n{split_line[MANUFACTURER_D_NAME]}"
                            maude_data[key][
                                MANUFACTURER_D_ADDRESS_1
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_ADDRESS_1]}"
                            maude_data[key][
                                MANUFACTURER_D_ADDRESS_2
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_ADDRESS_2]}"
                            maude_data[key][MANUFACTURER_D_CITY] += f"\nChange:\n{split_line[MANUFACTURER_D_CITY]}"
                            maude_data[key][
                                MANUFACTURER_D_STATE_CODE
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_STATE_CODE]}"
                            maude_data[key][
                                MANUFACTURER_D_ZIP_CODE
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_ZIP_CODE]}"
                            maude_data[key][
                                MANUFACTURER_D_ZIP_CODE_EXT
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_ZIP_CODE_EXT]}"
                            maude_data[key][
                                MANUFACTURER_D_COUNTRY_CODE
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_COUNTRY_CODE]}"
                            maude_data[key][
                                MANUFACTURER_D_POSTAL_CODE
                            ] += f"\nChange:\n{split_line[MANUFACTURER_D_POSTAL_CODE]}"
                            maude_data[key][DEVICE_OPERATOR] += f"\nChange:\n{split_line[DEVICE_OPERATOR]}"
                            maude_data[key][
                                EXPIRATION_DATE_OF_DEVICE
                            ] += f"\nChange:\n{split_line[EXPIRATION_DATE_OF_DEVICE]}"
                            maude_data[key][MODEL_NUMBER] += f"\nChange:\n{split_line[MODEL_NUMBER]}"
                            maude_data[key][CATALOG_NUMBER] += f"\nChange:\n{split_line[CATALOG_NUMBER]}"
                            maude_data[key][LOT_NUMBER] += f"\nChange:\n{split_line[LOT_NUMBER]}"
                            maude_data[key][OTHER_ID_NUMBER] += f"\nChange:\n{split_line[OTHER_ID_NUMBER]}"
                            maude_data[key][DEVICE_AVAILABILITY] += f"\nChange:\n{split_line[DEVICE_AVAILABILITY]}"
                            maude_data[key][
                                DATE_RETURNED_TO_MANUFACTURER
                            ] += f"\nChange:\n{split_line[DATE_RETURNED_TO_MANUFACTURER]}"
                            maude_data[key][
                                DEVICE_REPORT_PRODUCT_CODE
                            ] += f"\nChange:\n{split_line[DEVICE_REPORT_PRODUCT_CODE]}"
                            maude_data[key][DEVICE_AGE_TEXT] += f"\nChange:\n{split_line[DEVICE_AGE_TEXT]}"
                            maude_data[key][
                                DEVICE_EVALUATED_BY_MANUFACTUR
                            ] += f"\nChange:\n{split_line[DEVICE_EVALUATED_BY_MANUFACTUR]}"
                            maude_data[key][
                                COMBINATION_PRODUCT_FLAG
                            ] += f"\nChange:\n{split_line[COMBINATION_PRODUCT_FLAG]}"
                            maude_data[key][UDI_DI] += f"\nChange:\n{split_line[UDI_DI]}"
                            maude_data[key][UDI_PUBLIC] += f"\nChange:\n{split_line[UDI_PUBLIC]}"
                    except IndexError:
                        # TODO: add some error logging here so we aren't failing siletly?
                        pass

    return maude_data, header


def parse_foitext(
    path: pathlib.Path, maude_data: dict[str, list[str]], header: list[str]
) -> tuple[dict[str, list[str]], list[str]]:
    idx = indexer()
    MDR_REPORT_KEY = next(idx)
    MDR_TEXT_KEY = next(idx)
    TEXT_TYPE_CODE = next(idx)
    PATIENT_SEQUENCE_NUMBER = next(idx)
    DATE_REPORT = next(idx)
    FOI_TEXT = next(idx)
    maude_data = pre_fill(maude_data, FOI_TEXT)
    change_file = None
    header_add: list[str] = []
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
                    header_add = (
                        first.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")[MDR_TEXT_KEY:]
                    )
                for line in iter(i.readline, b""):
                    try:
                        split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                        key = line[MDR_REPORT_KEY]
                        if key in maude_data:
                            maude_data[key][MDR_TEXT_KEY] += f"{split_line[MDR_TEXT_KEY]}"
                            maude_data[key][TEXT_TYPE_CODE] += f"{split_line[TEXT_TYPE_CODE]}"
                            maude_data[key][PATIENT_SEQUENCE_NUMBER] += f"{split_line[PATIENT_SEQUENCE_NUMBER]}"
                            maude_data[key][DATE_REPORT] += f"{split_line[DATE_REPORT]}"
                            maude_data[key][FOI_TEXT] += f"{split_line[FOI_TEXT]}"

                    except IndexError:
                        # TODO: add some error logging here so we aren't failing silently.
                        pass
    if change_file:
        with open(change_file, "r") as f:
            print(f"reading file {change_file.name}")
            i = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            first = i.readline()
            for line in iter(i.readline, b""):
                try:
                    split_line = line.decode("utf-8", errors="backslashreplace").rstrip("\r\n").split("|")
                    key = split_line[MDR_REPORT_KEY]
                    if key in maude_data:
                        maude_data[key][MDR_TEXT_KEY] += f"\nChange:\n{split_line[MDR_TEXT_KEY]}"
                        maude_data[key][TEXT_TYPE_CODE] += f"\nChange:\n{split_line[TEXT_TYPE_CODE]}"
                        maude_data[key][PATIENT_SEQUENCE_NUMBER] += f"\nChange:\n{split_line[PATIENT_SEQUENCE_NUMBER]}"
                        maude_data[key][DATE_REPORT] += f"\nChange:\n{split_line[DATE_REPORT]}"
                        maude_data[key][FOI_TEXT] += f"\nChange:\n{split_line[FOI_TEXT]}"
                except IndexError:
                    # TODO: add some error logging here so we aren't failing silently.
                    pass
    header.extend(header_add)
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
