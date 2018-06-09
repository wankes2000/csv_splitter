import argparse
import pandas as pd
import ntpath
from zipfile import ZipFile
import zipfile
from os.path import basename


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
    args = parser.parse_args()
    df = pd.read_csv(args.input, iterator=True, chunksize=args.rows)
    filename = ntpath.basename(args.input)
    count = 0
    for chunk in df:  # for each 100k rows
        output_file = '{}/{}{}'.format(args.output, str(count), filename)
        chunk.to_csv('{}/{}{}'.format(args.output, str(count), filename), index=None)

        # Zip compression has not yet implement in pandas to_csv method
        with ZipFile('{}.zip'.format(output_file), 'w', zipfile.ZIP_DEFLATED) as myzip:
            myzip.write(output_file, basename(output_file))
        print('Processed chunk {}'.format(str(count)))
        count += 1


if __name__ == "__main__":
    main()
