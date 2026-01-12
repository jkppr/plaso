# -*- coding: utf-8 -*-
"""An output module that saves events to Fluent Bit."""

from plaso.output import manager
from plaso.output import shared_fluentbit


class FluentBitOutputModule(shared_fluentbit.SharedFluentBitOutputModule):
  """Output module for Fluent Bit."""

  NAME = 'fluentbit'
  DESCRIPTION = 'Saves the events into a Fluent Bit server.'

  def WriteFieldValues(self, output_mediator, field_values):
    """Writes field values to the output.

    Args:
      output_mediator (OutputMediator): mediates interactions between output
          modules and other components, such as storage and dfVFS.
      field_values (dict[str, str]): output field values per name.
    """
    if self._index_name:
      field_values['__ts_searchindex'] = self._index_name

    self._event_documents.append(field_values)
    self._number_of_buffered_events += 1

    if self._number_of_buffered_events > self._flush_interval:
      self._FlushEvents()

  def WriteHeader(self, output_mediator):
    """Connects to the Fluent Bit server.

    Args:
      output_mediator (OutputMediator): mediates interactions between output
          modules and other components, such as storage and dfVFS.
    """
    self._Connect()


manager.OutputManager.RegisterOutput(FluentBitOutputModule)
