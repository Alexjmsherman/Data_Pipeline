__author__ = 'alsherman'

import logging


logger = logging.getLogger(__name__)


class CSVCreator:
    """ creates and exports a csv from input data """

    def __init__(self, source_metadata):
        """
        :param source_metadata: includes the data to export
        """

        self.source_metadata = source_metadata
        self.csv_name = source_metadata.output_path

    def create_csv(self, sources_metadata):
        """ export one or more csvs

        :param sources_metadata: metadata about the source
        """

        # if only one dataframe, add it to a dict to add name
        if not isinstance(self.source_metadata.dataframe, dict):
            self.source_metadata.dataframe = {self.csv_name:self.source_metadata.dataframe}

        for csv_name, dataframe in self.source_metadata.dataframe.items():
            csv_name = self.source_metadata.create_output_path(csv_name, sources_metadata)
            dataframe.to_csv(csv_name, index=False)
            logger.info('Created CSV: {}'.format(csv_name))


if __name__ == "__main__":
    import logging.config
    logging.config.fileConfig(r"Logging/logging.conf")
