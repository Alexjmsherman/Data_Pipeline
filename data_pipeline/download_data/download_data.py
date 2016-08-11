__author__ = 'alsherman'

import urllib
import zipfile
import gzip
import requests
import logging


logger = logging.getLogger(__name__)


class DownloadData:
    """ includes various methods to download data depending on the data format """

    def __init__(self, source_metadata):
        """
        :param source_metadata: includes metadata about a data source, including the source data location and data format
        """

        self.local = source_metadata.local
        self.raw_data_path = source_metadata.raw_data_path
        self.download_type = source_metadata.download_type

    def download(self, download_type):
        """ determines which download type to call
        :param download_type: specifies source data format
        :return: downloaded data or None for local files
        """

        if self.local:
            return None

        downloaders = {'gzip':self._download_and_extract_gzip,
                       'zip':self._download_and_extract_zip_data,
                       'url':self._download_and_extract_url,
                       'ftp':self._download_ftp}

        return downloaders[download_type]()

    def _download_and_extract_zip_data(self, list_of_files_to_skip=[], records_to_extract=None):
        """ download and extract zip file

        :param list_of_files_to_skip: notates files not to skip in the zip
        :param records_to_extract: notates the number of rows of data to extract, defaults uses all data

        :return: data extracted from a zip
        """

        zfile = self._download_zip_helper()

        for filename in zfile.namelist()[0:records_to_extract]:  # get name of zipfile
            if filename in list_of_files_to_skip:
                continue

            with zfile.open(filename) as f:
                logger.info('Collected zip_file: {}'.format(filename))
                return f.readlines()  # extract data

    def _download_zip_helper(self):
        """ helper function to download a zip and handle errors

        :return: downloaded zip file
        """

        try:
            zfile, _ = urllib.urlretrieve(self.raw_data_path)  # download zip file
            logger.info('Collected Zfile: {}'.format(zfile))
        except DownloadError as e:
            logger.info('URL ERROR: {} no longer a valid URL'.format(self.raw_data_path))
        zfile = zipfile.ZipFile(zfile)  # open zip file
        return zfile

    def _download_and_extract_gzip(self):
        """ download and extract gzip with a single file

        :return: opened gzip file
        """

        try:
            gzip_file, _ = urllib.urlretrieve(self.raw_data_path)  # download gzip file
            logger.info('Collected gzip_file: {}'.format(gzip_file))
            decompressedFile = gzip.GzipFile(gzip_file)  # open gzip file
        except DownloadError as e:
            logger.info('URL ERROR: {} no longer a valid URL'.format(self.raw_data_path))

        return decompressedFile.readlines()

    def _download_and_extract_url(self):
        """ download data from a given url

        :return: downloaded text data from url
        """

        url = self.raw_data_path

        try:
            r = requests.get(url)
            logger.info('collected data from {}'.format(url))
        except DownloadError as e:
            logger.info('URL ERROR: {} is no longer a valid URL'.format(url))

        data = r.text
        return data

    def _download_ftp(self):
        """ download data from ftp

        :return: downloaded data
        """

        url = self.raw_data_path

        # TODO: need to refacto urllib2 into requests
        try:
            request = urllib2.Request(url)
            logger.info('collected data from {}'.format(url))
        except DownloadError as e:
            logger.info('URL ERROR: {} is no longer a valid URL'.format(url))
        r = urllib2.urlopen(request)
        data = (row for row in r)

        return data


class DownloadError(Exception):
    def __init__(self):
        Exception.__init__(self, "Download error occurred")