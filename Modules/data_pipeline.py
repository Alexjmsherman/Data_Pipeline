__author__ = "alsherman"

import logging
from Get_Data import download_data
from Sources_Metadata.source_metadata import SourceMetadata
from Export_Data.create_csv import CSVCreator


logger = logging.getLogger(__name__)


class DataPipeline:
    """ ETL process to run for each data source. Downloads data, Cleans data, and creates a CSV  """

    def __init__(self, name, sources_metadata, data_category, func, local=False):
        self.name = name
        self.sources_metadata = sources_metadata
        self.data_category = data_category
        self.local = local
        self._func = func

    def run_pipeline(self):
        """ get dataset metadata, download data, clean data, and export data """
        logger.info('start {}'.format(self.name))

        source_metadata = SourceMetadata(sources_metadata=self.sources_metadata, data_category=self.data_category, local=self.local)
        downloader = download_data.DownloadData(source_metadata)
        source_metadata.downloaded_data = downloader.download(source_metadata.download_type)
        source_metadata.dataframe = self._func(source_metadata)
        CSVCreator(source_metadata).create_csv(self.sources_metadata)

        logger.info('completed {}'.format(self.name))

