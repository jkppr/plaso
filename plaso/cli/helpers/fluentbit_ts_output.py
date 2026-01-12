# -*- coding: utf-8 -*-
"""The Fluent Bit Timesketch output module CLI arguments helper."""

from plaso.cli.helpers import interface
from plaso.cli.helpers import manager
from plaso.cli.helpers import fluentbit_output
from plaso.lib import errors
from plaso.output import fluentbit_ts


class FluentBitTimesketchOutputArgumentsHelper(interface.ArgumentsHelper):
  """Fluent Bit Timesketch output module CLI arguments helper."""

  NAME = 'fluentbit_ts'
  CATEGORY = 'output'
  DESCRIPTION = 'Argument helper for the Fluent Bit Timesketch output module.'

  @classmethod
  def AddArguments(cls, argument_group):
    """Adds command line arguments the helper supports to an argument group.

    This function takes an argument parser or an argument group object and adds
    to it all the command line arguments this helper supports.

    Args:
      argument_group (argparse._ArgumentGroup|argparse.ArgumentParser):
          argparse group.
    """
    fluentbit_output.FluentBitOutputArgumentsHelper.AddArguments(
        argument_group)

    argument_group.add_argument(
        '--timeline_id', '--timeline-id',
        dest='timeline_id', type=int,
        action='store',
        metavar='IDENTIFIER', help=(
             'The identifier of the timeline in Timesketch.'))

  @classmethod
  def ParseOptions(cls, options, output_module):  # pylint: disable=arguments-renamed
    """Parses and validates options.

    Args:
      options (argparse.Namespace): parser options.
      output_module (OutputModule): output module to configure.

    Raises:
      BadConfigObject: when the output module object is of the wrong type.
      BadConfigOption: when a configuration parameter fails validation.
    """
    if not isinstance(
        output_module, fluentbit_ts.FluentBitTimesketchOutputModule):
      raise errors.BadConfigObject(
          'Output module is not an instance of FluentBitTimesketchOutputModule')

    fluentbit_output.FluentBitOutputArgumentsHelper.ParseOptions(
        options, output_module)

    timeline_identifier = cls._ParseNumericOption(options, 'timeline_id')

    if timeline_identifier:
      output_module.SetTimelineIdentifier(timeline_identifier)


manager.ArgumentHelperManager.RegisterHelper(
    FluentBitTimesketchOutputArgumentsHelper)
