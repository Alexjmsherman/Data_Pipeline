__author__ = 'alsherman'

import pandas as pd
import csv
import logging


logger = logging.getLogger(__name__)


class DataframeCreator:
    """ Restructures and converts data from many formats (txt, zip, url, ...) into a pandas dataframe """

    def __init__(self, source_metadata, sep=',', txt_helper=None, columns=None, header=0, names=None):
        """
        :param source_metadata: contains metadata about data source
        :param sep: raw data separator
        :param txt_helper: used for txt files that do not follow conventional dataframe tabular format
        :param columns: dataframe columns
        :param header: default 0 means that the first row of the file is the headers
        :param names: column headers, passed in manually
        """

        self.local = source_metadata.local
        self.raw_data_path = source_metadata.raw_data_path
        self.download_type = source_metadata.download_type
        self.downloaded_data = source_metadata.downloaded_data
        self.columns = columns
        self.header = header
        self.names = names
        self.sep = sep
        self.txt_helper = txt_helper

    def create_dataframe(self):
        """ create a dataframe from a local or external source

        :returns: raw data dataframe
        """

        if self.txt_helper:
            return pd.DataFrame(self.txt_helper, columns=self.columns)
        elif self.local:
            temp_df = pd.read_csv(self.raw_data_path, sep=self.sep, header=self.header, names=self.names)
            return temp_df
        elif self.download_type == 'url':
            df = pd.DataFrame(self.yield_urllib2_helper())
        elif self.download_type == 'ftp':
            df = pd.DataFrame(self.yield_ftp_helper())
        else:
            df = pd.DataFrame([row for row in csv.reader(self.yield_zip_helper(), delimiter=self.sep)])

        if self.names:
            df.columns = self.names
        else:
            logger.info("Current Dataframe headers - {} ".format(df.columns))
            logger.info("New Dataframe headers {}".format(df.values[0]))
            df.columns = df.values[0]
            df.drop(0, inplace=True)

        return df

    def yield_zip_helper(self):
        for row in self.downloaded_data:
            yield row

    def yield_urllib2_helper(self):
        raw_data = []
        for row in self.downloaded_data.split('\n'): # separate each column to convert into DataFrame
            raw_data.append(row.split(self.sep))
        return raw_data

    def yield_ftp_helper(self):
        raw_data = []
        for row in self.downloaded_data:  # separate each column to convert into DataFrame
            raw_data.append(row.split(self.sep))
        return raw_data


if __name__ == '__main__':
    logging.config.fileConfig(r"Logging/logging.conf")
