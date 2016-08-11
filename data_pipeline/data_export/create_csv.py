__author__ = 'alsherman'

import logging
import logging.config


logger = logging.getLogger(__name__)


class CSVCreator:
    """ creates a csv from input data """

    def __init__(self, source_metadata):
        """
        :param source_metadata: provides the source data
        """
        self.source_metadata = source_metadata
        self.csv_name = source_metadata.output_path

    def create_csv(self, sources_metadata):
        if isinstance(self.source_metadata.dataframe, dict):
            self.create_many_csvs(sources_metadata)
        else:
            self.create_single_csv()

    def create_single_csv(self):
        self.source_metadata.dataframe.to_csv(self.csv_name, index=False)
        logger.info('Created CSV: {}'.format(self.csv_name))

    def create_many_csvs(self, sources_metadata):
        for key, val in self.source_metadata.dataframe.items():
            csv_name = self.source_metadata.create_output_path(key, sources_metadata)
            val.to_csv(csv_name, index=False)
            logger.info('Created CSV: {}'.format(csv_name))

if __name__ == "__main__":
    logging.config.fileConfig(r"Logging/logging.conf")
