# -*- coding: utf-8 -*-
"""An output module that saves events to Fluent Bit for Timesketch."""

from plaso.output import logger
from plaso.output import manager
from plaso.output import shared_fluentbit


class FluentBitTimesketchOutputModule(
    shared_fluentbit.SharedFluentBitOutputModule):
  """Output module for Timesketch Fluent Bit."""

  NAME = 'fluentbit_ts'
  DESCRIPTION = (
      'Saves the events into a Fluent Bit server for use '
      'with Timesketch.')

  def __init__(self):
    """Initializes an output module."""
    super(FluentBitTimesketchOutputModule, self).__init__()
    self._timeline_identifier = None

  def WriteFieldValues(self, output_mediator, field_values):
    """Writes field values to the output.

    Args:
      output_mediator (OutputMediator): mediates interactions between output
          modules and other components, such as storage and dfVFS.
      field_values (dict[str, str]): output field values per name.
    """
    # Add timeline_id on the event level. It is used in Timesketch to
    # support shared indices.
    field_values['__ts_timeline_id'] = self._timeline_identifier
    if self._index_name:
      field_values['__ts_searchindex'] = self._index_name

    self._event_documents.append(field_values)
    self._number_of_buffered_events += 1

    if self._number_of_buffered_events > self._flush_interval:
      self._FlushEvents()

  def GetMissingArguments(self):
    """Retrieves a list of arguments that are missing from the input.

    Returns:
      list[str]: names of arguments that are required by the module and have
          not been specified.
    """
    if not self._timeline_identifier:
      return ['timeline_id']
    return []

  def SetTimelineIdentifier(self, timeline_identifier):
    """Sets the timeline identifier.

    Args:
      timeline_identifier (int): timeline identifier.
    """
    self._timeline_identifier = timeline_identifier
    logger.info('Timeline identifier: {0:d}'.format(self._timeline_identifier))

  def WriteHeader(self, output_mediator):
    """Connects to the Fluent Bit server.

    Args:
      output_mediator (OutputMediator): mediates interactions between output
          modules and other components, such as storage and dfVFS.
    """
    self._Connect()


manager.OutputManager.RegisterOutput(FluentBitTimesketchOutputModule)
