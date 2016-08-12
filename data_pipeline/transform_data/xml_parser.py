__author__ = 'alsherman'

import logging
from itertools import product
import pandas as pd


logger = logging.getLogger(__name__)


class XmlElementParser:
    """ Parses XML, extracting selected elements and attributes.

    Requires that the XML has a linear structure in which there can be many children, but only single grandparents
    and parents in the hierarchy.

    Valid XML: <grandparent>
                 <parent>
                   <child1>item1</child1>
                   <child2>item2</child2>
                 </parent>
               </grandparent>

    Invalid XML: <grandparent>
                   <parent1>
                     <child1>item2</child1>
                   </parent1>
                   <parent2>
                     <child2>item1</child2>
                   </parent2>
                 </grandparent>
    """

    all_data = {}

    def __init__(self, item):
        """
        :param item: an XML section to parse for inner elements and attributes. Each XML file may have many elements
        """
        self.item = item

    def extract_data(self, data_list, row_start=None):
        """
        :param data_list: class dict that stores all lists to append extracted rows. Lists are converted to dataframes
               in the create dataframes method
        :param row_start - used if additional items should be prepended to the start of the row (e.g. unique id or name)
               that does not exist in the xml item passed to the parser
        """
        if len(data_list[0]) != 3:
            logger.error('data list is missing or has excess children_names, parent_names, or class_list: {}'.format(data_list[0]))
            raise ValueError('data list is missing or has excess children_names, parent_names, or a class_list')

        for children_names, parent_names, class_list in data_list:
            logger.debug('row_start_items: {} | parent: {}'.format(row_start, parent_names))

            self.get_nested(children_names=children_names,
                            parent_names=parent_names,
                            class_list=class_list,
                            row_start=row_start)

    def get_nested(self, children_names, parent_names, class_list, row_start=None):
        """ Search in parent_names to determine the level of nesting. Collect each element in children_names and
         create a cartesian product to add all combinations of children XML elements to the class_list.

        :param children_names: select xml elements to extract - children in the xml tree hierarchy
        :param parent_names: Contains the XML hierarchy to search. Inner elements (children) exist inside parents.
               Notes: None if persistent parent == overall container (e.g. self.item) as no nested elements exist
        :param class_list: List to append each completed row. When complete, class_list is often converted to a pandas dataframe
        :param row_start - used if additional items should be prepended to the start of the row (e.g. unique id or name)
                           that does not exist in the xml item passed to the parser

        Notes: create a list of lists containing all values of each attribute then get the cartesian product. This is
        important for one to many mappings to get all possible pairings, does nothing special for one to one mappings
        """

        # check if the row should include values not included in the xml item passed to the parser
        if row_start is None:
            row = []
        else:
            row = row_start

        # determine the nested structure of xml and filter to the level of the child element of interest
        if isinstance(parent_names, list):
            if len(parent_names) < 2:
                logger.info('persistent parent should only be a list if there are at least two parents: {}'.format(parent_names))
                raise TypeError('persistent parent should only be a list if there are at least two parents')

            grandparents = parent_names[0:-1]
            parent = parent_names[-1]

            # if parent names is a list, then there should always be at least one grandparent
            first_grandparent = grandparents[0]
            grandparent = self.item.find(first_grandparent)
            # if multiple grandparents, traverse to the last parent, containing the children elements of interest
            if len(grandparents) >= 2:
                other_grandparents = grandparents[1:]
                for next_grandparent in other_grandparents:
                    grandparent = grandparent.find(next_grandparent)

            parent_xml = grandparent.find_all(parent)
        elif parent_names is None:
            # used when parent category == self.item
            # (no need to get nested elements as persistent parent is the top level of the hierarchy)
            parent_xml = self.item
        else:
            # means that there is only one persistent parent
            # every element shares a parent: <parent><item1></item1><item2></item2></parent>
            parent_xml = self.item.find(parent_names)

        # ignore categories that do not exist in the raw data
        if parent_xml is None:
            logger.info('parent: {} is None for row: {}'.format(parent_names, row))
            return

        # if only one parent or one category, cast the string to a list to avoid iterating over characters of a string
        if not isinstance(children_names, list):
            children_names = [children_names]
        if not isinstance(parent_xml, list):
            parent_xml = [parent_xml]

        # create a list of lists, then get the cartesian product. This is important for one to many mappings
        # to get all possible pairings; does nothing special for one to one mappings
        for selected_xml in parent_xml:

            children_list = []
            for xml_elem in children_names:

                searching_for_attribute = isinstance(xml_elem, tuple)
                if searching_for_attribute:
                    element = xml_elem[0]
                    attribute = xml_elem[1]

                    if parent_names is None:
                        # searching for attribute on highest level of xml elements - no need to find inner elements
                        result = self.item[attribute]
                    else:
                        result = selected_xml.find(element)[attribute]

                    # turn into a list as I extract the first list element for other child_list during cartesian product
                    children_list.append([result])
                    continue

                # search for an element (not an attribute)
                children = selected_xml.find_all(xml_elem)
                if len(children) == 0:
                    logger.info('parent and child xml elements share name (e.g. <a><a><instance></a></a>),'
                                ' skipping value: {}'.format(xml_elem))
                    continue

                child_list = []
                for child in children:
                    # if there are more than one of the child xml element
                    try:
                        result = child.text
                    except TypeError('Child is None in XML parser') as e:
                        logger.info(e)
                        result = ''

                    child_list.append(result)
                children_list.append(child_list)

            # happens if child and parent share name (e.g. <a><a><instance></a></a>)
            if len(children_list) == 0:
                logger.info('child and parent share name, skipping row')
                continue

            # get all combinations (cartesian product) of elements in children list
            # convert tuple in list to a list itself and add row ids, if exist
            complete_row = row[:] + list(list(product(*children_list))[0])
            class_list.append(complete_row)

    @classmethod
    def create_dataframes(cls, sources_metadata):
        """ converts every list in all_data dict (a dict of lists for each row of parsed xml) into dataframes

        :param sources_metadata: contains column headers used to create the dataframe
        """

        for name, data_list in cls.all_data.items():

            logger.debug('list: {} is about to be converted into a dataframe'.format(name))

            if len(data_list) == 0:
                # if there is no data, replace the list with the files headers
                cls.all_data[name] = sources_metadata['headers'][name]
            else:
                # convert the list into a dataframe to export from CsvCreator
                cls.all_data[name] = pd.DataFrame(data_list, columns=sources_metadata['headers'][name])
