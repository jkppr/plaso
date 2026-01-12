# -*- coding: utf-8 -*-
"""Shared functionality for Fluent Bit output modules."""

import requests

from acstore.containers import interface as containers_interface

from dfdatetime import interface as dfdatetime_interface

from dfvfs.serializer.json_serializer import JsonPathSpecSerializer

from plaso.output import interface
from plaso.output import logger
from plaso.output import shared_opensearch


class SharedFluentBitOutputModule(interface.OutputModule):
  """Shared functionality for a Fluent Bit output module."""

  # pylint: disable=abstract-method

  NAME = 'fluentbit_shared'

  SUPPORTS_ADDITIONAL_FIELDS = True
  SUPPORTS_CUSTOM_FIELDS = True

  _DEFAULT_FLUSH_INTERVAL = 1000

  # Number of seconds to wait before a request to Fluent Bit is timed out.
  _DEFAULT_REQUEST_TIMEOUT = 300

  _DEFAULT_FIELD_NAMES = [
      'datetime',
      'display_name',
      'message',
      'source_long',
      'source_short',
      'tag',
      'timestamp',
      'timestamp_desc']

  def __init__(self):
    """Initializes an output module."""
    super(SharedFluentBitOutputModule, self).__init__()
    self._custom_fields = {}
    self._event_documents = []
    self._field_names = self._DEFAULT_FIELD_NAMES
    self._field_formatting_helper = (
        shared_opensearch.SharedOpenSearchFieldFormattingHelper())
    self._flush_interval = self._DEFAULT_FLUSH_INTERVAL
    self._host = None
    self._index_name = None
    self._number_of_buffered_events = 0
    self._port = None
    self._url_prefix = None

  def _Connect(self):
    """Connects to a Fluent Bit server.

    Raises:
      RuntimeError: if the server cannot be reached.
    """
    logger.debug(f'Configured Fluent Bit server: {self._host:s} port: {self._port:d}')

  def SetIndexName(self, index_name):
    """Sets the index name.

    Args:
      index_name (str): name of the index.
    """
    self._index_name = index_name
    logger.debug(f'Fluent Bit index name: {index_name:s}')

  def _FlushEvents(self):
    """Inserts the buffered event documents into Fluent Bit."""
    if not self._event_documents:
      return

    url = f'http://{self._host}:{self._port:d}'
    if self._url_prefix:
      url = f'{url}/{self._url_prefix}'

    try:
      # Fluent Bit HTTP input expects a JSON payload.
      response = requests.post(
          url, json=self._event_documents, timeout=self._DEFAULT_REQUEST_TIMEOUT)

      if response.status_code not in (200, 201):
        logger.warning(
            f'Unable to send events to Fluent Bit. Status code: {response.status_code}, '
            f'Response: {response.text}')
      else:
        logger.debug(
            f'Inserted {self._number_of_buffered_events:d} events into Fluent Bit')

    except requests.exceptions.RequestException as exception:
      logger.warning(f'Unable to send events with error: {exception!s}')

    self._event_documents = []
    self._number_of_buffered_events = 0

  def _SanitizeField(self, data_type, attribute_name, field):
    """Sanitizes a field for output.

    Args:
      data_type (str): event data type.
      attribute_name (str): name of the event attribute.
      field (object): value of the field to sanitize.

    Returns:
      object: sanitized value of the field.
    """
    # Some parsers have written bytes values to storage.
    if isinstance(field, bytes):
      field = field.decode('utf-8', 'replace')
      logger.warning((
          f'Found bytes value for attribute: {attribute_name:s} of data type: '
          f'{data_type!s}. Value was converted to UTF-8: "{field:s}"'))

    return field

  def Close(self):
    """Closes connection to Fluent Bit.

    Inserts any remaining buffered event documents.
    """
    self._FlushEvents()

  def GetFieldValues(
      self, output_mediator, event, event_data, event_data_stream, event_tag):
    """Retrieves the output field values.

    Args:
      output_mediator (OutputMediator): mediates interactions between output
          modules and other components, such as storage and dfVFS.
      event (EventObject): event.
      event_data (EventData): event data.
      event_data_stream (EventDataStream): event data stream.
      event_tag (EventTag): event tag.

    Returns:
      dict[str, str]: output field values per name.
    """
    event_values = {}

    if event_data:
      for attribute_name, attribute_value in event_data.GetAttributes():
        # Ignore attribute container identifier and date and time values.
        if isinstance(attribute_value, (
            containers_interface.AttributeContainerIdentifier,
            dfdatetime_interface.DateTimeValues)):
          continue

        if (isinstance(attribute_value, list) and attribute_value and
            isinstance(attribute_value[0],
                       dfdatetime_interface.DateTimeValues)):
          continue

        # Ignore protected internal only attributes.
        if attribute_name[0] == '_' and attribute_name != '_parser_chain':
          continue

        # Output _parser_chain as parser for backwards compatibility.
        if attribute_name == '_parser_chain':
          attribute_name = 'parser'

        event_values[attribute_name] = attribute_value

    if event_data_stream:
      for attribute_name, attribute_value in event_data_stream.GetAttributes():
        event_values[attribute_name] = attribute_value

    for attribute_name in self._field_names:
      if attribute_name not in event_values:
        event_values[attribute_name] = None

    field_values = {}
    for attribute_name, attribute_value in event_values.items():
      if attribute_name == 'path_spec':
        try:
          field_value = JsonPathSpecSerializer.WriteSerialized(attribute_value)
        except TypeError:
          continue

      else:
        field_value = self._field_formatting_helper.GetFormattedField(
            output_mediator, attribute_name, event, event_data,
            event_data_stream, event_tag)

      if field_value is None and attribute_name in self._custom_fields:
        field_value = self._custom_fields.get(attribute_name, None)

      if field_value is None:
        field_value = '-'

      field_values[attribute_name] = self._SanitizeField(
          event_data.data_type, attribute_name, field_value)

    return field_values

  def SetAdditionalFields(self, field_names):
    """Sets the names of additional fields to output.

    Args:
      field_names (list[str]): names of additional fields to output.
    """
    self._field_names.extend(field_names)

  def SetCustomFields(self, field_names_and_values):
    """Sets the names and values of custom fields to output.

    Args:
      field_names_and_values (list[tuple[str, str]]): names and values of
          custom fields to output.
    """
    self._custom_fields = dict(field_names_and_values)
    self._field_names.extend(self._custom_fields.keys())

  def SetFlushInterval(self, flush_interval):
    """Sets the flush interval.

    Args:
      flush_interval (int): number of events to buffer before doing a bulk
          insert.
    """
    self._flush_interval = flush_interval
    logger.debug(f'Fluent Bit flush interval: {flush_interval:d}')

  def SetServerInformation(self, server, port):
    """Sets the server information.

    Args:
      server (str): IP address or hostname of the server.
      port (int): Port number of the server.
    """
    self._host = server
    self._port = port
    logger.debug(f'Fluent Bit server: {server!s} port: {port:d}')

  def SetURLPrefix(self, url_prefix):
    """Sets the URL prefix.

    Args:
      url_prefix (str): URL prefix.
    """
    self._url_prefix = url_prefix
    logger.debug(f'Fluent Bit URL prefix: {url_prefix!s}')
