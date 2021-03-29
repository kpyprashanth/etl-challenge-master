import logging
from database import Database
import os.path
from pathlib import Path
from datetime import datetime

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)


class EtlScript:
    def __init__(self):
        self.database_conn = Database("acme")
        self.header_file = "headers.txt"
        self.data_file = "data.csv"
        self.out_file = "output.csv"

    def load_file_to_database(self, file_path: str):
        self.database_conn.load_file(file_path)

    def run(self):
        # Initialize file pointers
        header_fp = data_fp = out_fp = None
        try:
            #Check if we have recieved the latest data and header files
            if not (os.path.exists(self.data_file) and os.path.exists(self.header_file)):
                raise ValueError("Today's files are not yet available in the root folder. Please check")

            # Read headers file and format as pipe seperated
            with open(self.header_file, mode="r") as header_fp:
                lines = [line.rstrip() for line in header_fp]
            headers = "|".join(lines)
            columns_count = headers.count("|") + 1 # get columns count in headers file
            logger.debug("Header file containing " + str(columns_count)+" columns are: " + headers)

            # Read data file and write headers and data files to output file formatted accordingly
            with open(self.data_file, 'r') as data_fp, open(self.out_file, 'w') as out_fp:
                out_fp.write(headers+"\n")
                for line in data_fp:
                    if line.count("|") + 1 != columns_count:# DQ check to see if we have same number of columns across the data file
                        logger.debug(line)
                        raise ValueError("Columns count mismatch, Header column count is " + str(
                            columns_count) + " Data file column count is " + str(line.count("|") + 1))
                    out_fp.write(line)
            logger.info("Output file generated successfully")

            # Load DQ checked file to database
            self.load_file_to_database(self.out_file)

            # Archive todays files to Archive folder. This helps in checking if we have received today's files
            Path("Archive").mkdir(parents=True, exist_ok=True)
            Path(self.data_file).rename("Archive/"+self.data_file[:-4]+datetime.now().strftime('%Y%m%d')+".csv")
            Path(self.header_file).rename("Archive/"+self.header_file[:-4]+datetime.now().strftime('%Y%m%d')+".csv")
            logger.info("Today's files have been archived")

        except FileNotFoundError as error:
            logger.error(error)
        except ValueError as error:
            logger.error(error)
        except Exception as error:
            logger.error(error)

        finally:
            # Close opened file handles
            for fp in [header_fp, data_fp, out_fp]:
                if fp:
                    fp.close()


if __name__ == "__main__":
    EtlScript().run()