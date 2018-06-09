import argparse
import pandas as pd
from ntpath import basename
from zipfile import ZipFile
import zipfile
import os


def main():
    """
    This script split large csv file in multiple files using Pandas and compress the output in Zip format
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="csv file to be splited", required=True)
    parser.add_argument("-s", "--separator", help="csv separator", default=",")
    parser.add_argument("-r", "--rows", help="split file in chunks of r rows", default=10000, type=int)
    parser.add_argument("-o", "--output", help="output path of the splited files", required=True)
    parser.add_argument("-t", "--test", help="test chunk sizes", default=False)
    args = parser.parse_args()
    df = pd.read_csv(args.input, iterator=True, chunksize=args.rows)
    filename = basename(args.input)
    count = 0
    if args.test:
        __write_output(df.get_chunk(), filename, args.rows, args.output)
    else:
        for chunk in df:
            __write_output(chunk, filename, count, args.output)
            print('Processed chunk {}'.format(str(count)))
            count += 1


def __write_output(chunk, filename, count, output):
    output_file = '{}/{}{}'.format(output, str(count), filename)
    chunk.to_csv('{}/{}{}'.format(output, str(count), filename), index=None)

    # Zip compression has not yet implement in pandas to_csv method
    with ZipFile('{}.zip'.format(output_file), 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(output_file, basename(output_file))
        os.remove(output_file)


if __name__ == "__main__":
    main()
