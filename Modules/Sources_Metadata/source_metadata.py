__author__ = 'alsherman'

import os
import logging
import configparser

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read('config.ini')


class SourceMetadata:
    """ stores metadata about a data source. Includes information on how to download data (i.e. data format and
    source location) and where to export data (i.e. location of local folder on users computer) """

    users_local_raw_data_folder = config['USER']['DataExportFolder']
    local = use_local_files = config['SOURCEMETADATA']['use_local_files']

    def __init__(self, sources_metadata, data_category, local=False):
        """
        :param sources_metadata: dict with static information about the data source
        :param data_category: lists category for file of interest, used when one source has many data files
        :param local: specifies whether to use a local file or download data (used for testing purposes)
        """

        self._local = local
        self._source_path = sources_metadata['source_path']
        self._data_categories = sources_metadata['data_categories']
        self.download_type = sources_metadata['data_categories'][data_category]['download_type']
        self.source_name = sources_metadata['source_name']
        self.full_name = sources_metadata['full_name']
        self.website = sources_metadata['website']
        self.headers = sources_metadata['headers'][data_category]
        self.data_category = data_category
        self.output_path = os.path.join(self.users_local_raw_data_folder, sources_metadata['output_path'][data_category])
        self.downloaded_data = None
        self.dataframe = None
        self.sources_metadata = sources_metadata

    def __repr__(self):
        return "<class: raw_data>: {} - {}".format(self.full_name, self.data_category)

    @property
    def local(self):
        """ determine if data is on the local machine or stored externally
        NOTE: set SourceMetadata.local to True during testing to use local files for the entire program
        """

        if SourceMetadata.local == True:
            return True
        else:
            return self._local

    @property
    def raw_data_path(self):
        """ create and return the path to the raw data file """

        start_path = os.path.join(self.users_local_raw_data_folder, self._source_path)
        path = None
        if self.local:
            path = os.path.join(start_path, self._data_categories[self.data_category]['local'])
        else:
            try:
                path = self._data_categories[self.data_category]['external']
            except KeyError:
                logger.error("{} does not have a link to an external data source. Use a path to local data instead".format(self.__repr__))
        return path

    def create_output_path(self, data_category, sources_metadata):
        """ return the output path for a given data source
        :param data_category: lists category for file of interest, used when one source has many data files
        :param sources_metadata: dict with static information about the data source
        :return: output path for a given data source
        """

        output_path = os.path.join(self.users_local_raw_data_folder, sources_metadata['output_path'][data_category])
        return output_path
